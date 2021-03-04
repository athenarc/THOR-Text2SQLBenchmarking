package discover.testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.Set;

import discover.DiscoverApplication;
import discover.components.CandidateNetworksGenerator;
import discover.components.MasterIndex;
import discover.components.Parser;
import discover.components.PlanGenerator;
import discover.components.TupleSetPostProcessor;
import discover.components.executors.PlanExecutor;
import discover.model.FreeTupleSet;
import discover.model.JoiningNetworkOfTupleSets;
import discover.model.OverloadedTuple;
import discover.model.TupleSet;
import discover.model.TupleSetGraph;
import discover.model.execution.Assignment;
import discover.model.execution.CandidateNetworkAssignment;
import discover.model.execution.ExecutionPlan;
import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DataSourceFactory;

// import shared.database.connectivity.DatabaseConfigurations;
import shared.database.model.DatabaseType;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;
import shared.util.Stopwords;
import shared.util.Timer;

import discover.testing.PerformanceMonitor;

public class TestingMain {

    private static final String DEBUG_FILE = "Discover.txt";
    private static final String QUERY_FILE = "./qqc";

    static HashMap<String, Pair<SQLDatabase, SchemaGraph>> storedDatabases = new HashMap<>();
    


    public static void main(String[] args) throws FileNotFoundException {        
        // Redirect all output to file
		PrintStream originalOut = System.out;
		PrintStream o = new PrintStream(new File(DEBUG_FILE));        

        // Load properties and queries
        PropertiesSingleton.loadPropertiesFile("app");
        ArrayList<Boolean> dflag = new ArrayList<>();
        List<String> overloadedQueries = getQueryList(QUERY_FILE);    
        int maxNetworkSize = 2;
        int maxResults = 10;
        dflag.add(true);

        // A delay to open the visual vm
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to start:");
        scanner.nextLine();

        // Read the db's
        readAndStoreDatabases(new String[]{"mysql.IMDB", "mysql.YELP", "mysql.MAS"});

        
        // The SCVLine to print
        CSVManager.init();
        // List<CSVLine> csvLines = new ArrayList<>();

        // Execute the queries
        for (String ovQuery: overloadedQueries) {
            CSVLine line = new CSVLine();
            Timer timer = new Timer(Timer.Type.WALL_CLOCK_TIME);
            String query = line.query = ovQuery.split(";")[0];
            String schemaName = ovQuery.split(";")[1];
            
            String name = schemaName.split("\\.")[1];
            String type = schemaName.split("\\.")[0];

            //  (name, DatabaseType.getTypeFromString(type));
            DataSourceFactory.loadConnectionProperties(name, DatabaseType.getTypeFromString(type));
            SQLDatabase database = storedDatabases.get(schemaName).getLeft();
            SchemaGraph schemaGraph = storedDatabases.get(schemaName).getRight(); 
            
            System.out.println("[INFO] Running query: " + query + " ...");
            try {                                  
                System.setOut(o);     // Swap to file print
                System.out.println("Query : " + query);
                
                // Parse the query into keywords and print them.                 
                List<String> keywords = Parser.whitespaceTokenizer(query); 
                keywords.removeIf(k -> ( Stopwords.isStopword(k) ));
                printlnD("Keywords: " + keywords + "\n", dflag);
                line.numOfKeyword = keywords.size();
                
                // ================================
                // Create the MasterIndex instance and print the basic tuple sets.
                
                // Performance inits
                PerformanceMonitor monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();

                timer.start();
                MasterIndex masterIndex = new MasterIndex(keywords, database);
                List<TupleSet> basicTupleSets = masterIndex.generateBasicTupleSets();                

                    // Set stats
                    line.timerPerFunction.add(timer.stop());
                    line.mappingsPerKW = masterIndex.getKeywordsToNumberOfTuples();
                    Set<String> relations = new HashSet<>();
                    for (TupleSet set : basicTupleSets)
                        relations.add(set.getTable().getName());
                    line.numOfRelations = relations.size();
                    line.sqlIO = masterIndex.getTotalRowsOfSqlIO();
                    line.cpuIndex = monitor.calcCpuPer();
                    line.memIndex = monitor.calcMemUsage();
                
                
                // Prints
                if (DiscoverApplication.DEBUG_PRINTS) {
                    printlnD("=====================================\n", dflag);
                    printlnD("BASIC TUPLE SETS\n", dflag);
                    for (TupleSet set : basicTupleSets) {
                        printlnD(set.toAbbreviation() + " : " + set.getSize(), dflag);
                        if (dflag.get(0))
                            // if (set.getSize() < 20)
                            //     set.print();
                        printlnD("", dflag);
                    }
                }

                // ========================
                // Create postProcessor Instance and print all TupleSets for all subsets of K and all relations

                // start cpu time
                monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                timer.start();

                TupleSetPostProcessor postProcessor = new TupleSetPostProcessor(keywords, basicTupleSets);
                List<TupleSet> keywordSubsetTupleSets = postProcessor.generateKeywordSubsetsTupleSets();
                
                    // set stats
                    line.timerPerFunction.add(timer.stop());
                    line.numOfTupleSets = keywordSubsetTupleSets.size();

                // Create a list with all the free and non-free tuple sets.
                List<TupleSet> freeAndNonFreeTupleSets = new ArrayList<TupleSet>();
                freeAndNonFreeTupleSets.addAll(keywordSubsetTupleSets);
                freeAndNonFreeTupleSets.addAll(FreeTupleSet.getFreeTupleSets(database.getTables()));
                
                // Prints
                if (DiscoverApplication.DEBUG_PRINTS) {
                    printlnD("=====================================\n", dflag);
                    printlnD("TUPLE SET POST PROCESSOR", dflag);
                    printlnD("Keywords subsets Ri^K for each relation\n", dflag);
                    for (TupleSet set : keywordSubsetTupleSets) {
                        printlnD(set.toAbbreviation(), dflag);
                        if (dflag.get(0))
                            if (set.getSize() < 20)
                                set.print();
                        printlnD("", dflag);
                    }
                }


                // =================================
                // Create the tupleSets graph.
                TupleSetGraph tupleSetGraph = new TupleSetGraph();
                tupleSetGraph.fill(freeAndNonFreeTupleSets, schemaGraph);
                
                printlnD("\n=====================================\n", dflag);
                printlnD("Tuple Set Graph\n", dflag);
                printlnD(tupleSetGraph.toString(), dflag);
                
                // ================================
                // Initialize the candidate networks generator component.
                printlnD("=====================================\n", dflag);
                printlnD("Candidate Network Generator\n", dflag);
                
                timer.start();
                CandidateNetworksGenerator candidateNetworksGenerator = new CandidateNetworksGenerator(
                    keywordSubsetTupleSets,
                    tupleSetGraph,
                    keywords,
                    maxNetworkSize
                );

                // Generate the networks.
                candidateNetworksGenerator.generateCandidateNetworks();
                    
                    // Set stats
                    line.timerPerFunction.add(timer.stop());
                    line.cnetworks = candidateNetworksGenerator.getCandidateNetworks().size();
                    line.cpuCNetGen = monitor.calcCpuPer();
                    line.memCNetGen = monitor.calcMemUsage();

                // Print the candidate networks.
                if (DiscoverApplication.DEBUG_PRINTS) {
                    printlnD("Candidate Networks\n", dflag);
                    for (JoiningNetworkOfTupleSets jnts : candidateNetworksGenerator.getCandidateNetworks()) {
                        printlnD(jnts + "\n", dflag);
                    }
                }

                // Prune candidate networks
                List<JoiningNetworkOfTupleSets> candidateNetworks = candidateNetworksGenerator.getCandidateNetworks();                
                // if (candidateNetworks.size() > 20)
                //     candidateNetworks = candidateNetworks.subList(0, 20);
                    

                
                // =========================================
                // Initialize the plan generator and generate the execution plan.         
                printlnD("=====================================\n", dflag);
                printlnD("ExecutionPlan\n", dflag);
                monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                timer.start();                
                PlanGenerator planGenerator = new PlanGenerator();
                ExecutionPlan executionPlan = null;

                if (DiscoverApplication.USE_INTERMEDIATE_RESULTS) {
                    executionPlan = planGenerator.generateExecutionPlan(candidateNetworks);                
                }
                else {
                    executionPlan = planGenerator.generateExecutionPlan_CN_ONLY(candidateNetworks);
                }

                    // Set stats
                    line.timerPerFunction.add(timer.stop());                    
                    Integer count = 0;
                    for (Assignment assignment : executionPlan.getAssignments())
                        if (assignment instanceof CandidateNetworkAssignment)
                            count++;
                    line.sqlQueries = count;
                    line.cpuPlanGen = monitor.calcCpuPer();
                    line.memPlanGen = monitor.calcMemUsage();


                // Debug Prints
                printlnD(executionPlan.toString(), dflag);

                // ===================
                // Plan Executor
                // Create the plan executor and execute the plan.
                printlnD("=====================================\n", dflag);
                printlnD("Plan Executor:\n", dflag);
                timer.start();
                PlanExecutor planExecutor = new PlanExecutor(schemaGraph, database, maxResults);
                planExecutor.execute(executionPlan, keywordSubsetTupleSets);

                    // set stats
                    line.timerPerFunction.add(timer.stop());
                    line.numOfTuples = planExecutor.getAllResults().size();
                    line.tuples = planExecutor.getOrderedResults();


                // Print results 
                if (DiscoverApplication.DEBUG_PRINTS)
                    planExecutor.printStats(executionPlan);

                List<OverloadedTuple> results = planExecutor.getResults();
                if (results.isEmpty())
                    System.out.println("[INFO] We could not produce a results");
                else  {
                    System.out.println("[INFO] Print results");
                    for (OverloadedTuple ot: results)
                        System.out.println(ot);
                }
                
                // csvLines.add(line);
                CSVManager.toFile(line);

                // Swap back to original output stream
                // System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                System.setOut(originalOut);
            }
            catch(Exception e) {
                System.out.println("[ERROR] Exception occurred while running query: " + query);
                e.printStackTrace();
            }

            System.out.println("\tDone...");            
        }        

        // CSVManager.toFile(overloadedQueries, csvLines);
    }

    /** Read queries */
    public static List<String> getQueryList(String filePath) {
        List<String> queries = new ArrayList<>();

        // Get queries 
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filePath));
            String line;

            // Read the lines.
            while ((line = reader.readLine()) != null) {
                if (!line.contains("#"))  // # means comment
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

    // print debugs 
    public static void printlnD(String message, ArrayList<Boolean> flag) {
        if (flag.get(0) == true)
            System.out.println(message);
    } 

    public static void printD(String message, ArrayList<Boolean> flag) {
        if (flag.get(0) == true)
            System.out.print(message);
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
            String name = dbName.split("\\.")[1];
            String type = dbName.split("\\.")[0];

            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(name, DatabaseType.getTypeFromString(type));


            // Create PK-FK Relationship Graph.
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // Store the db.
            storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }

}