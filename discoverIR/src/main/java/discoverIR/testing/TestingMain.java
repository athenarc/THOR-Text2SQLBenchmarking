package discoverIR.testing;

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

import discoverIR.DiscoverIRApplication;
import discoverIR.components.CandidateNetworksGenerator;
import discoverIR.components.IREngine;
import discoverIR.components.Parser;
import discoverIR.components.execution.engines.NaiveExecutionEngine;
import discoverIR.model.ExecutionEngineAlgorithms;
import discoverIR.model.FreeTupleSet;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.Parameters;
import discoverIR.model.TupleSet;
import discoverIR.model.TupleSetGraph;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseConfigurations;
import shared.database.connectivity.DatabaseInfo;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;
import shared.util.Stopwords;
import shared.util.Timer;

public class TestingMain {
    
    private static final String DEBUG_FILE = "DiscoverIR.txt";
    private static final String QUERY_FILE = "./q";

    static HashMap<String, Pair<SQLDatabase, SchemaGraph>> storedDatabases = new HashMap<>();

    public static void main(String[] args) throws FileNotFoundException {
        // Redirect all output to file
		PrintStream o = new PrintStream(new File(DEBUG_FILE));
		PrintStream originalOut = System.out;

        // Load properties and queries
        CSVManager.init();
        ArrayList<Boolean> dflag = new ArrayList<>();
        List<String> overloadedQueries = getQueryList(QUERY_FILE);
        int maxTuples = 10;
        int maxNetworkSize = 3;
        dflag.add(true);

        // A delay to open the visual vm
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to start:");
        scanner.nextLine();

        // Read and store know databases for testing        
        readAndStoreDatabases(new String[]{"IMDB", "YELP", "MAS"});
        
        // Execute the queries
        for (String ovQuery: overloadedQueries) {
            CSVLine queryStats = new CSVLine();
            Timer timer = new Timer(Timer.Type.WALL_CLOCK_TIME);
            String query = queryStats.query = ovQuery.split(";")[0];
            String schemaName = ovQuery.split(";")[1];
            
            // Automatically get the info of the database.
            useDatabase(schemaName);
            SQLDatabase database = storedDatabases.get(schemaName).getLeft();
            SchemaGraph schemaGraph = storedDatabases.get(schemaName).getRight();
            
            System.out.println("[INFO] Executing: " + query);
			try {				
                System.setOut(o);     // Swap to file print                                

                // Parse the query into keywords and print them.                 
                List<String> keywords = Parser.whitespaceTokenizer(query);        
                keywords.removeIf(word ->(Stopwords.isStopword(word)));
                System.out.println("Keywords: " + keywords + "\n");
                queryStats.numOfKeyword = keywords.size();

                // Execution Parameters.
                Parameters parameters = new Parameters.ParametersBuilder(keywords)
                    .setAndSemantics(true)
                    .setPrintResultsOrderedByTable(false)            
                    .setMaxTuples(maxTuples)
                    .setMaxNetworksSize(maxNetworkSize)
                    .setExecutionEngineAlgorithm(ExecutionEngineAlgorithms.GlobalPipelined)
                    .build();

                // ================================
                // Create the MasterIndex instance and print the basic tuple sets.
                PerformanceMonitor monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                timer.start();
                IREngine IREngine = new IREngine(parameters.keywords, database);
                List<TupleSet> tupleSets = IREngine.generateTupleSets(query);

                    // Set stats
                    queryStats.timerPerFunction.add(timer.stop());
                    queryStats.totalMappings = 0;
                    Set<String> relations = new HashSet<>();
                    for (TupleSet set : tupleSets) {
                        relations.add(set.getTable().getName());
                        queryStats.totalMappings++;
                    }
                    queryStats.numOfRelations = relations.size();
                    queryStats.numOfTupleSets = tupleSets.size();
                    queryStats.sqlIO = IREngine.getSqlIoRows();
                    queryStats.cpuIndex = monitor.calcCpuPer();
                    queryStats.memIndex = monitor.calcMemUsage();
                
                // Prints
                if (DiscoverIRApplication.DEBUG_PRINTS) {
                    printlnD("=====================================\n", dflag);
                    printlnD("BASIC TUPLE SETS\n", dflag);
                    for (TupleSet set : tupleSets) {
                        printlnD(set.toAbbreviation() + " : " + set.getSize(), dflag);
                        if (dflag.get(0))
                            // set.print(true);
                        printlnD("", dflag);
                    }
                }

                // Create a list with all the free and non-free tuple sets.
                List<TupleSet> freeAndNonFreeTupleSets = new ArrayList<TupleSet>();
                freeAndNonFreeTupleSets.addAll(tupleSets);
                freeAndNonFreeTupleSets.addAll(FreeTupleSet.getFreeTupleSets(database.getTables()));
                
                // =================================
                // Create the tupleSets graph.
                monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
                timer.start();

                // Create the tuple set graph.
                TupleSetGraph tupleSetGraph = new TupleSetGraph();
                tupleSetGraph.fill(freeAndNonFreeTupleSets, schemaGraph);
                
                // ================================
                // Initialize the candidate networks generator component.
                printlnD("=====================================\n", dflag);
                printlnD("Candidate Network Generator\n", dflag);
                CandidateNetworksGenerator candidateNetworksGenerator = new CandidateNetworksGenerator(
                    tupleSets,
                    tupleSetGraph,
                    parameters.keywords,
                    parameters.maxNetworksSize
                );

                // Generate the networks.
                List<JoiningNetworkOfTupleSets> candidateNetworks = candidateNetworksGenerator.generate(parameters.andSemantics);

                    // Set stats
                    queryStats.timerPerFunction.add(timer.stop());
                    queryStats.cnetworks = candidateNetworks.size();
                    queryStats.sqlQueries = candidateNetworks.size();
                    queryStats.cpuCNetGen = monitor.calcCpuPer();
                    queryStats.memCNetGen = monitor.calcMemUsage();

                // Prune CNs
                // if (candidateNetworks.size() > 20)
                //     candidateNetworks = candidateNetworks.subList(0, 20);

                // Print the candidate networks.
                if (DiscoverIRApplication.DEBUG_PRINTS) {
                    printlnD("Candidate Networks\n", dflag);
                    for (JoiningNetworkOfTupleSets jnts : candidateNetworks) {
                        printlnD(jnts + "\n", dflag);
                    }
                }            

                // =========================================
                // Plan Executor
                // Create the plan executor and execute the plan.
                printlnD("=====================================\n", dflag);
                printlnD("Plan Executor:\n", dflag);                
                timer.start(); // Start the timer.

                // Initialize an execution engine
                NaiveExecutionEngine executionEngine = new NaiveExecutionEngine(
                    candidateNetworks, schemaGraph, database, tupleSets,
                    parameters.maxTuples, parameters.keywords,
                    parameters.andSemantics, parameters.printResultsOrderedByTable,
                    parameters.efficientPlanGenerator
                );

                // Execute the networks.
                // executionEngine.execute();

                    // set stats
                    queryStats.timerPerFunction.add(timer.stop());
                    queryStats.numOfTuples = executionEngine.getAllResults().size();
                    queryStats.tuples = executionEngine.getAllTopKCNTuples();
                
                // Write stat to file
                CSVManager.toFile(queryStats);

                // System.out.println("\n\n\n^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^\n");

                // Swap back to original output stream
                System.setOut(originalOut);
            } 
            catch (Exception e) {
                System.out.println("[ERR] Failed ...");
                e.printStackTrace();
            }
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
     *  Set the database to be used.
     */
    static void useDatabase(String databaseName) {
        DatabaseConfigurations configs = new DatabaseConfigurations("app", databaseName);     // "app" is the file name of the properties file           
        DataSourceFactory.loadDbConfigurations(configs);        
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

            // Automatically get the info of the database.
            SQLDatabase database = DatabaseInfo.getDatabaseObject(dbName);

            // Create PK-FK Relationship Graph.
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // Store the db.
            storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }

}