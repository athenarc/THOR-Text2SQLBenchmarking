package spark.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseConfigurations;
import shared.database.connectivity.DatabaseInfo;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;
import shared.util.Stopwords;
import shared.util.Timer;
import shared.util.Timer.Type;
import spark.SparkApplication;
import spark.components.BlockPipelineExecutionEngine;
import spark.components.CandidateNetworksGenerator;
import spark.components.IREngine;
import spark.components.Parser;
import spark.model.FreeTupleSet;
import spark.model.JoiningNetworkOfTupleSets;
import spark.model.OverloadedTupleList;
import spark.model.Parameters;
import spark.model.TupleSet;
import spark.model.TupleSetGraph;

public class TestingMain {

    private static final String DEBUG_FILE = "Spark.txt";
    // private static final String QUERY_FILE = "../../queries/ALLkwq_withdbs.txt";
    private static final String QUERY_FILE = "./q";
    
    private static HashMap<String, Pair<SQLDatabase, SchemaGraph>> storedDatabases = new HashMap<>();

    public static void main(String[] args) throws FileNotFoundException {
        // Redirect all output to file
		PrintStream o =  new PrintStream(new File(DEBUG_FILE));	
		PrintStream originalOut = System.out;

        // Initialize variables.
        Timer timer = new Timer(Type.WALL_CLOCK_TIME);    // Used to measure the tim e of different parts of the program.
        List<String> overloadedQueries = getQueryList(QUERY_FILE);        
        Integer maxTuples = 30;
        Integer maxNetworkSize = 5;

        // A delay to open the visual vm
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to start:");
        scanner.nextLine();

        // Initialize the scv manager
        CSVManager.init();

        // Read and store all the databases
        readAndStoreDatabases(new String[]{"IMDB", "YELP", "MAS"});
                
        // Loop all overloadedQueries
        for (String ovQuery: overloadedQueries) {
            
            String query = ovQuery.split(";")[0];
            String schemaName = ovQuery.split(";")[1];

            // Use tha database we got from the input
            useDatabase(schemaName);
            SQLDatabase database = storedDatabases.get(schemaName).getLeft();
            SchemaGraph schemaGraph = storedDatabases.get(schemaName).getRight();

            System.out.println("[INFO] Executing: " + query);
			
			try {				
                System.setOut(o);                         // Swap to file print                 
                CSVLine line = new CSVLine();
                line.query = query;
                
                // Parse the query into keywords and remove the stopwords.
                List<String> keywords = Parser.whitespaceTokenizer(query);
                keywords.removeIf(word ->(Stopwords.isStopword(word)));
                // System.out.println("Query: " + query + " - Keywords: " + keywords + "\n");
                // System.out.println("=========================");

                    // Set stats
                    line.numOfKeyword = keywords.size();                    
                
                // Set the execution Parameters.
                Parameters parameters = new Parameters.ParametersBuilder(keywords)
                    .setAndSemantics(true)
                    .setPrintResultsOrderedByTable(true)            
                    .setMaxTuples(maxTuples)
                    .setMaxNetworksSize(maxNetworkSize)                
                    .build();
                    
                // ================== 
                if (SparkApplication.DEBUG_PRINTS == true) {
                    System.out.println("================================");
                    System.out.println("IR Engine");
                    System.out.println("================================");
                }

                // Get the tuple set of every table.
                PerformanceMonitor monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                timer.start();
                IREngine IREngine = new IREngine(parameters.keywords, database);
                List<TupleSet> nonFreeTupleSets = IREngine.generateTupleSets(query);

                    // Set statistics
                    line.timerPerFunction.add(timer.stop());
                    line.totalMappings = 0;
                    Set<String> relations = new HashSet<>();
                    for (TupleSet set : nonFreeTupleSets) {
                        relations.add(set.getTable().getName());
                        line.totalMappings ++;
                    }
                    line.numOfRelations = relations.size();
                    line.numOfTupleSets = nonFreeTupleSets.size();
                    line.sqlIO = IREngine.getRowsOfIoSql();
                    line.cpuIndex = monitor.calcCpuPer();
                    line.memIndex = monitor.calcMemUsage();

                // Debug Prints
                if (SparkApplication.DEBUG_PRINTS == true) {
                    System.out.println("\nTable tuple sets\n");
                    for (TupleSet set : IREngine.getTupleSets()) {
                        System.out.println(set.toAbbreviation());
                        if (set.getSize() < 20)
                            set.print(false); 
                        System.out.println();
                    }
                    System.out.println("\nIR Engine Time " + timer.stop() + "(s)");
                }
                // -----------
                    

                // ============================
                if (SparkApplication.DEBUG_PRINTS == true) {
                    System.out.println("================================");
                    System.out.println("Candidate Network Generator");
                    System.out.println("================================");
                }

                // Generate the free tuple sets and merge them with the non-free ones, in a list returned by the IREngine.
                timer.start();
                List<TupleSet> freeAndNonFreeTupleSets = new ArrayList<TupleSet>();
                freeAndNonFreeTupleSets.addAll(nonFreeTupleSets);
                freeAndNonFreeTupleSets.addAll(FreeTupleSet.getFreeTupleSets(database.getTables()));
                
                // Create the tuple set graph.
                monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                TupleSetGraph tupleSetGraph = new TupleSetGraph();
                tupleSetGraph.fill(freeAndNonFreeTupleSets, schemaGraph);                

                // Initialize the candidate networks generator component.
                CandidateNetworksGenerator candidateNetworksGenerator = new CandidateNetworksGenerator(
                    nonFreeTupleSets,
                    tupleSetGraph,
                    parameters.keywords,
                    parameters.maxNetworksSize
                );

                // Generate the networks.                
                List<JoiningNetworkOfTupleSets> candidateNetworks = candidateNetworksGenerator.generate(parameters.andSemantics);                

                    // Set statistics
                    line.timerPerFunction.add(timer.stop());
                    line.numOfCandidateNetworks = candidateNetworks.size();
                    line.cpuCNetGen = monitor.calcCpuPer();
                    line.memCNetGen = monitor.calcMemUsage();

                // Prune the candidate Networks
                // if (candidateNetworks.size() > 70)
                //     candidateNetworks = candidateNetworks.subList(0, 20);
                

                // Debug Prints
                if (SparkApplication.DEBUG_PRINTS == true) {
                    System.out.println("\nCandidate Network Generator Time " + timer.stop() + "(s)");
                    System.out.println("\n----------------------------------\n");
                    for (JoiningNetworkOfTupleSets network : candidateNetworks) {
                        System.out.println(network + "\n");
                    }
                    candidateNetworksGenerator.printStats();
                    JoiningNetworkOfTupleSets.printStats();
                }
                // ---------                
                
                // =================================                
                if (SparkApplication.DEBUG_PRINTS) {
                    System.out.println("================================");
                    System.out.println("Execution");
                    System.out.println("================================");
                }
                timer.start();
                
                // Initialize the execution engine and execute the networks with the selected algorithm.
                BlockPipelineExecutionEngine executionEngine = new BlockPipelineExecutionEngine(
                    candidateNetworks, schemaGraph, database, parameters.maxTuples,
                    parameters.keywords, parameters.efficientPlanGenerator, nonFreeTupleSets,
                    parameters.andSemantics, parameters.printResultsOrderedByTable
                );

                // Execute the networks.
                // executionEngine.execute();

                    // Set statistics
                    line.sqlQueries = executionEngine.getNumOfSqlQueriesExecuted();
                    line.timerPerFunction.add(timer.stop());
                    line.numOfTuples = executionEngine.getAllResults().size();
                    line.topRes = executionEngine.getResults();
                
                // Debug Prints                
                // System.out.println("ALL FINAL RESULTS");
                // System.out.println("================================\n");
                // for (OverloadedTupleList tl: executionEngine.getAllTopKCNTuples())
                //     tl.print(true);
                // ----------------                

                CSVManager.toFile(line);
            }
            catch (Exception e) {
                System.out.println("[ERR] Failed ...");
                e.printStackTrace();
            }
            finally {
                // Swap back to original output stream
                System.setOut(originalOut);
            }
			System.out.println("\tDone...");
        }        
    }

    /**
     *  Set the database to be used.
     */
    static void useDatabase(String databaseName) {
        DatabaseConfigurations configs = new DatabaseConfigurations("app", databaseName);     // "app" is the file name of the properties file           
        DataSourceFactory.loadDbConfigurations(configs);        
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
            // Use tha database we got from the input
            useDatabase(dbName);

            // Automatically get the info of database.
            SQLDatabase database = DatabaseInfo.getDatabaseObject(dbName);

            // Create PK-FK Relationship Graph.
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // Store the db.
            storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }

}
