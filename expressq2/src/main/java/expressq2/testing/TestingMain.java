package expressq2.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;

import expressq2.components.Parser;
import expressq2.components.QueryAnalyzer;
import expressq2.components.QueryInterpreter;
import expressq2.components.QueryTranslator;
import expressq2.components.SQLQueryExecutor;
import expressq2.model.AnnotatedQuery;
import expressq2.model.Keyword;
import expressq2.model.OverloadedTupleList;
import expressq2.model.QueryPattern;
import expressq2.model.SQLQuery;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseConfigurations;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.ORMSchemaGraph;
import shared.util.OrderedList;
import shared.util.Pair;
import shared.util.Stopwords;
import shared.util.Timer;
import shared.util.Timer.Type;

public class TestingMain {

	static final String DEBUG_FILE = "Expressq2.txt";
	private static final String QUERY_FILE = "q";


	private static HashMap<String, Pair<SQLDatabase, ORMSchemaGraph>> storedDatabases = new HashMap<>();
 
    public static void main(String[] args) throws IOException {
        // Load the database configurations from the configurations file.
        DatabaseConfigurations.fill("app");

		// Redirect all output to file
		PrintStream o = new PrintStream(new File(DEBUG_FILE));
		PrintStream originalOut = System.out;

		// A delay to open the visual vm
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to start:");
        scanner.nextLine();

		// Read all THOR's databases
		readAndStoreDatabases(new String[]{"IMDB", "MAS", "YELP"});
		
		// Initialize the info writer
		InfoWriter.init();

		// Initialize variables	
		Timer timer = new Timer(Type.WALL_CLOCK_TIME), totalTimer = new Timer(Type.WALL_CLOCK_TIME);
		List<String> queries = getQueryList(QUERY_FILE);		
		int maxPatterns = 5;         // The maximum number of QueryInterpretations executed and returned to the user.	
		int maxTuples = 10;

		// Loop all the queries
        for (String ovQuery: queries) {
			String query = ovQuery.split(";")[0];
			String schemaName = ovQuery.split(";")[1];

			System.out.println("[INFO] Executing: " + query);

			// Connect to the database and retrieve its information.
			DatabaseConfigurations.useDatabase(schemaName);

            // Get the cached database and ORM graph
            Pair<SQLDatabase, ORMSchemaGraph> dbPair = storedDatabases.get(schemaName);
            SQLDatabase database = dbPair.getLeft();
            ORMSchemaGraph  schemaGraph = dbPair.getRight();
			
			try {				
				System.setOut(o);     // Swap to file print 
				totalTimer.start();   // Start the total timer
				Info answer = new Info(); // Create an (info) answer for the query execution.			
				answer.query = query;

				PerformanceMonitor monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
				timer.start();
				// Parse the query into keywords and print them.
				List<Keyword> keywords = Parser.whitespaceTokenizer(query);
				keywords.removeIf(word -> (Stopwords.isStopword(word.getTerm())));
				// System.out.println("[INFO] Keywords: " + keywords);

				// =============================================				
				// Analyze the query and annotate it.				
				QueryAnalyzer queryAnalyzer = new QueryAnalyzer(database, keywords, schemaGraph);           // The Query Analyzer
				List<AnnotatedQuery> annotatedQueries = queryAnalyzer.createAnnotatedQueries();            // The Annotated Queries

				// Check if the Annotated Queries are valid.            
				List<AnnotatedQuery> queriesToRemove = new ArrayList<>();
				for (AnnotatedQuery annotatedQuery: annotatedQueries) {
					// Store the non valid Queries to a List.
					if (!Parser.checkAnnotatedQueryValidity(annotatedQuery))
						queriesToRemove.add(annotatedQuery);            
				}
				// Remove the non valid Quires.
				annotatedQueries.removeAll(queriesToRemove);
				
					// Set stats
					answer.timerPerComponent.add(timer.stop());
					answer.keywords = keywords.size();
					answer.mappingsPerKW = queryAnalyzer.keywordNumOfMappingsMap;
					answer.numOfAnnotatedQueries = annotatedQueries.size();
					answer.sqlIO = queryAnalyzer.getNumOfIoSql();
                    answer.cpuQueryAn = monitor.calcCpuPer();
                    answer.memQueryAn = monitor.calcMemUsage();
				

				// =============================================        
				// Create the Query Patterns
				OrderedList<QueryPattern, Double> queryPatternList = new OrderedList<>(maxPatterns);  // Query Patterns

				// Loop each annotated Query and interpret it to query patterns.
				monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
				timer.start();				
				// int num = 0;
				for (AnnotatedQuery annotatedQuery: annotatedQueries) {       
					Timer innerTimer = new Timer(Type.WALL_CLOCK_TIME);
					// System.out.println("Q" + num++ + ":" + annotatedQuery.toString() + "\n");

					// Create all the Query Patterns extracted by the query.
					innerTimer.start();
					List<QueryPattern> pList = QueryInterpreter.interpret(annotatedQuery, schemaGraph);
					innerTimer.stop();

					// Get the score from each Query and Add them in the Queries List based on that score.
					for (QueryPattern p: pList) {                
					    double score = p.getScore();
					    queryPatternList.addElement(p, score);                
						// System.out.println("Pattern:\n" + p + "[RES] Score: " + score + "\n");
					}					

					//Note (just for the 1st ) : queryPatternList.addElement(pList.get(0), pList.get(0).getScore());
					// System.out.println("[INFO] Interpreted in: " + innerTimer + "\n------------------------------\n\n");
				}
				// System.out.println("-------------------------------");
				
					// Set statistics
					answer.timerPerComponent.add(timer.stop());	
					answer.numOfQueryPatters = queryPatternList.getElementsWithScore().size();
					answer.cpuQueryInter = monitor.calcCpuPer();
                    answer.memQueryInter= monitor.calcMemUsage();


					
				// =============================================
				// Translate, Execute and print the top K QueryPatterns.
				// System.out.println("[INFO] FINAL RESULTS");
				// System.out.println("\n+++++++++++TOP " + maxPatterns + " Queries+++++++++++++++++\n");
				monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
				Double translationTime = 0D, executionTime = 0D;
				Integer numOfResults = 0;
				

				// Dont run queries with > and < and =
				if (!query.contains(">") && !query.contains("<") && !query.contains("=")) {

					for (Pair<Double, QueryPattern> pattern: queryPatternList.getElementsWithScore()) {						
						QueryPattern p = pattern.getRight();                                 // The Query Pattern.            

						// Translate query pattern to sql
						timer.start();						 
						SQLQuery sqlQuery = QueryTranslator.translateQueryPattern(database, p, schemaGraph);
						translationTime += timer.stop();
						// System.out.println("Graph:\n" + p + "[RES] Query:\n" + sqlQuery + "\n[RES] Score: " + pattern.getLeft() + "\n");
						// System.out.println("[INFO] SQL query:\n" + sqlQuery + "\n");
						// System.out.println("[INFO] Translated to slq in: " + translationTime);

						// NOTE : I Have removed the select distinct subQuery on relationship relations cause it takes TOOOO much time.
						timer.start();
						// OverloadedTupleList results = SQLQueryExecutor.executeQuery(sqlQuery, database, pattern.getLeft());
						executionTime += timer.stop();                        
						// System.out.println("[INFO] Executed along an SQLEngine in: " + executionTimer + "\n");					
						
						// Store the results number.
						// if (results != null) {
							// System.out.println("[RES] ____ :");
							// numOfResults += results.getTupleList().size();

							// prune results if more than 100
							// results.pruneTupleList(20);
							// results.print(true);
							// System.out.println();
						// }
					}
				}
			
					// Set statistics
					answer.timerPerComponent.add(translationTime);
					answer.timerPerComponent.add(executionTime);
					answer.numOfResults = numOfResults;
					answer.cpuQueryTrans = monitor.calcCpuPer();
                    answer.memQueryTrans = monitor.calcMemUsage();

											

				// Write the answer to the file
				InfoWriter.toFile(answer);

				// Print a line 
				// System.out.println("\n+++++++++++++++++++++++++\n+++++++++++++++++++++++++\n\n");				
			} 
			catch (Exception e) {				
                System.out.println("[ERR] Failed ...");
				e.printStackTrace();				
			}			

			System.setOut(originalOut);
			System.out.println("\tDone...");						
		}
    }
    

	 /** Read queries */
	 static List<String> getQueryList(String filePath) {
        List<String> queries = new ArrayList<>();

        // Get queries 
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;

            // Read the lines.
            while ((line = reader.readLine()) != null) {
                queries.add(line);
            }

            // Close the reader.
            reader.close();
        } 
        catch (FileNotFoundException e) {
            System.out.println("[ERROR] Could not find queries file with name: '" + filePath + "'");
            System.exit(0);
        }
        catch (IOException e) {
            System.out.println("[ERROR] Could not read queries file with name: '" + filePath + "'");
            e.printStackTrace();
            System.exit(0);
        }

        return queries;
	}
	
	  /**
     * Reads all databases and creates a database object and a schema object for 
     * each database. Then stores them for later usage
     * 
     * @param databases
     */
    public static void readAndStoreDatabases(String[] databases) {
        storedDatabases = new HashMap<>();

        for (String dbName: databases) {            
            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(dbName);

            // Create PK-FK Relationship Graph.
            ORMSchemaGraph schemaGraph = new ORMSchemaGraph();
            schemaGraph.fill(database.getTables(), database.getFKConstrains());

            // Store the db.
            storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }
}