package discoverIR;

import discoverIR.components.IREngine;
import discoverIR.components.Parser;
import discoverIR.components.CandidateNetworksGenerator;
import discoverIR.components.ExecutionEngineInitializer;
import discoverIR.components.execution.engines.ExecutionEngine;
import discoverIR.model.TupleSet;
import discoverIR.model.TupleSetGraph;
import discoverIR.model.ExecutionEngineAlgorithms;
import discoverIR.model.FreeTupleSet;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.Parameters;

import shared.database.model.graph.SchemaGraph;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseInfo;
import shared.database.model.SQLDatabase;
import shared.database.connectivity.DatabaseConfigurations;
import shared.util.Pair;
import shared.util.Stopwords;
import shared.util.Timer;
import shared.util.Timer.Type;
import shared.connectivity.thor.input.InputHandler;
import shared.connectivity.thor.response.Component;
import shared.connectivity.thor.response.GeneralArchitecture;
import shared.connectivity.thor.response.Response;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DiscoverIRApplication {

    // Parameters
    String query;
    String schemaName;
    public static Parameters parameters = new Parameters.ParametersBuilder()
        .setAndSemantics(true)
        .setPrintResultsOrderedByTable(false)
        .setMaxNetworksSize(3)
        .setExecutionEngineAlgorithm(ExecutionEngineAlgorithms.Naive)
        .build();

    // Database Variables
    private HashMap<String, Pair<SQLDatabase, SchemaGraph>> storedDatabases = new HashMap<>();
    SQLDatabase database;
    SchemaGraph schemaGraph;

    // Execution Variables
    private List<TupleSet> tupleSets;
    private TupleSetGraph tupleSetGraph;
    private List<JoiningNetworkOfTupleSets> candidateNetworks;
    private List<OverloadedTuple> finalResults = new ArrayList<>();;

    // Static variables
    public static final Boolean DEBUG_PRINTS = false;

    public static void main(String[] args) {
        // Create an instance of the DiscoverIrApplication
        DiscoverIRApplication discoverIrApp = new DiscoverIRApplication();

        // Read and store all the databases
        // discoverIrApp.readAndStoreDatabases(new String[]{"IMDB", "YELP", "MAS"});

        // Instantiate Thor's input Handler
        InputHandler inputHandler = new InputHandler();
        Timer timer = new Timer(Type.WALL_CLOCK_TIME), totalTimer = new Timer(Type.WALL_CLOCK_TIME);  // Used to measure the CPU time of different parts of the program.

         // Loop till the condition brakes from inside the loop
         while(true) {
            totalTimer.start();
            discoverIrApp.resetVars();

            // Call Thor's read input
            inputHandler.readInput();

            // Read the parameters needed for the execution from the stdin.
            discoverIrApp.query = inputHandler.getQuery();
            discoverIrApp.schemaName = inputHandler.getSchemaName();
            discoverIrApp.parameters.maxTuples = inputHandler.getResultsNumber();
            if (inputHandler.shutDownSystem())
                break;

            // Create all the Component objects that mirror the
            // actual components of discover and will be displayed in THOR.
            GeneralArchitecture genArch = new GeneralArchitecture();
            Component irEngine = new Component("IR Engine");
            Component cnGenerator = new Component("Network Generator");
            Component executor = new Component("Executor");

            try {
                // Create Database and Schema Graph automatically
                discoverIrApp.getDatabaseAndSchemaGraph();

                // Parse query and create the TupleSets
                discoverIrApp.parseQueryAndCreateTupleSets(timer, irEngine, genArch);

                // Create candidate networks
                discoverIrApp.generateCandidateNetworks(timer, cnGenerator, genArch);

                // Execute the candidate networks
                discoverIrApp.executeCandidateNetworks(timer, executor, genArch);
            }
            catch (Exception e) {
                System.err.println("[ERR] Exception ocurred while executing : " + discoverIrApp.query);
                e.printStackTrace();
            }


            // ================
            // Connect to THOR
            // ===============

            // Create the architecture of the system.
            // irEngine.connectWith(cnGenerator, "Tuple sets");
            // cnGenerator.connectWith(executor, "Candidate networks");

            // // Create a response and sent it to THOR
            // Response<OverloadedTuple> response = new Response<OverloadedTuple>(
            //     "discoverir",
            //     "DiscoverIR",
            //     genArch,
            //     Arrays.asList(irEngine, cnGenerator, executor),
            //     discoverIrApp.finalResults
            // );
            // response.sendToTHOR();

            System.out.println("TOTAL TIME: " + timer.stop());
        }
    }

    /**
     * Reads all databases and creates a database object and a schema object for
     * each database. Then stores them for later usage
     *
     * @param databases
     */
    public void readAndStoreDatabases(String[] databases) {
        this.storedDatabases = new HashMap<>();

        for (String dbName: databases) {
            // Use tha database we got from the input
            useDatabase(dbName);

            // Automatically get the info of database.
            SQLDatabase database = DatabaseInfo.getDatabaseObject(dbName);

            // Create PK-FK Relationship Graph.
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // Store the db.
            this.storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }

    /**
     * Fills the Database and the Schema Graph variables
     */
    void getDatabaseAndSchemaGraph() {
        // If we have stored the database then dont read it again.
        // Else read the db and store it.
        if (this.storedDatabases != null && this.storedDatabases.containsKey(schemaName)) {
            Pair<SQLDatabase, SchemaGraph> dbPair = this.storedDatabases.get(schemaName);
            useDatabase(schemaName);
            this.database = dbPair.getLeft();
            this.schemaGraph = dbPair.getRight();
        }
        else {

            // Use tha database we got from the input
            useDatabase(schemaName);

            // Automatically get the info of database.
            this.database = DatabaseInfo.getDatabaseObject(schemaName);

            // Create PK-FK Relationship Graph.
            this.schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // store the db
            this.storedDatabases.put(this.schemaName, new Pair<>(this.database, this.schemaGraph));

            // Print Stats and Database.
            if (DEBUG_PRINTS) {
                System.out.println("Database Recognized:\n\n" + database);
                System.out.print("=====================================\n\n");
                System.out.println("FK-PK Relationship Graph\n\n" + schemaGraph);
                System.out.print("=====================================\n\n");
            }
        }
    }


    /**
     * Parses the Query to keywords breaking into whitespace chars and creates the
     * Tuple Sets using the IR Engine Component
     *
     * @param timer
     * @param irEngine
     */
    public void parseQueryAndCreateTupleSets(Timer timer, Component irEngine, GeneralArchitecture genArch) {
        timer.start();

        // Parse the query into keywords.
        this.parameters.keywords = Parser.whitespaceTokenizer(query);
        this.parameters.keywords.removeIf(word ->(Stopwords.isStopword(word)));

        // Get the tuple set of every table.
        IREngine IREngine = new IREngine(this.parameters.keywords, database);
        this.tupleSets = IREngine.generateTupleSets(query);

        // Set stats
        irEngine.setTime(timer.stop());
        irEngine.addComponentInfo(IREngine.getStatistics());
        genArch.setEntityMapperOutput(IREngine.getTablesContainingQuery());


        // Print debug prints
        if (DEBUG_PRINTS) {
            System.out.println("Query: " + query + " - Keywords: " + this.parameters.keywords + "\n");
            System.out.println("---------------------------------\n");
            System.out.println("\n=================\n");
            System.out.println("IREngine ");
            System.out.println("\n=================\n");
            System.out.println("Table tuple sets\n");
            for (TupleSet set : IREngine.getTupleSets()) {
                System.out.println(set.toAbbreviation());
                if (set.getSize() < 50)
                    set.print(true);
                System.out.println();
            }
            System.out.println("\nIR Engine Time : " + irEngine.getTime() + "(s)");
            System.out.println("---------------------------------\n");
        }
    }


     /**
     * Create the TupleSet Graph
     */
    void createTupleSetGraph() {
        // Generate the free tuple sets and merge them in a list
        // with the non-free ones returned by the IREngine.
        List<TupleSet> freeAndNonFreeTupleSets = new ArrayList<TupleSet>();
        freeAndNonFreeTupleSets.addAll(tupleSets);
        freeAndNonFreeTupleSets.addAll(FreeTupleSet.getFreeTupleSets(database.getTables()));

        // Create the tupleSets graph.
        this.tupleSetGraph = new TupleSetGraph();
        tupleSetGraph.fill(freeAndNonFreeTupleSets, schemaGraph);

        // Debug Prints
        if (DEBUG_PRINTS) {
            System.out.println("\n=====================================\n");
            System.out.println("Tuple Set Graph\n");
            System.out.println("\n=====================================\n");
            // System.out.println(tupleSetGraph);       // print of tuple set is bad
        }
    }


    /**
     * Generate the candidate network using Candidate Network Generator Component
     *
     * @param timer
     * @param cnGenerator
     */
    public void generateCandidateNetworks(Timer timer, Component cnGenerator, GeneralArchitecture genArch) {
        timer.start(); // Start the timer.

        // Create the TupleSetGraph
        this.createTupleSetGraph();

        // Initialize the candidate networks generator component.
        CandidateNetworksGenerator candidateNetworksGenerator = new CandidateNetworksGenerator(
            this.tupleSets,
            this.tupleSetGraph,
            this.parameters.keywords,
            this.parameters.maxNetworksSize
        );

        // Generate the networks.
        this.candidateNetworks = candidateNetworksGenerator.generate(parameters.andSemantics);

        // Get statistics
        cnGenerator.setTime(timer.stop());
        cnGenerator.addComponentInfo(candidateNetworksGenerator.getStatistics());
        genArch.setInterpretationGeneratorOutput(this.candidateNetworks.size());

        if (DEBUG_PRINTS) {
            System.out.println("================================");
            System.out.println("Candidate Network Generator");
            System.out.println("================================");
            System.out.println("Resulting networks:\n");
            for (JoiningNetworkOfTupleSets network : candidateNetworks) {
                System.out.println(network + "\n");
            }
            candidateNetworksGenerator.printStats();
            JoiningNetworkOfTupleSets.printStats();
            System.out.println("\nCandidate Network Generator Time: " + cnGenerator.getTime() + "(s)");
        }
    }


    /**
     * Execute the Candidate Networks using one of the execution engines
     *
     * @param Timer
     * @param executor
     */
    public void executeCandidateNetworks(Timer timer, Component executor, GeneralArchitecture genArch) {
        timer.start();

        // Initialize an execution engine
        ExecutionEngine executionEngine = ExecutionEngineInitializer.initializeEngine(
            this.parameters,
            this.candidateNetworks,
            this.schemaGraph,
            this.database,
            this.tupleSets
        );

        // Execute the networks.
        executionEngine.execute();
        this.finalResults = executionEngine.getResults();

        // Set statistics
        executor.setTime(timer.stop());
        executor.addComponentInfo(executionEngine.getStatistics());
        genArch.setEntityMapperOutput(this.candidateNetworks.size());  // Each CN will be translated to an SQL query.

        // The results are printed from inside the Execution Engine
        // System.out.println("[INFO] Results\n");
        // for (SQLTuple tuple : finalResults)
        //     System.out.println(tuple);
        // System.out.println("\nExecution Time: " +  executor.getTime() + "(s)");
    }


    /** Reset the variables before each query  */
    public void resetVars() {
        this.tupleSets = null;
        this.tupleSetGraph = null;
        this.candidateNetworks = null;
        this.finalResults = new ArrayList<>();;
    }

    /**
     *  Set the database to be used.
     */
    static void useDatabase(String databaseName) {
        DatabaseConfigurations configs = new DatabaseConfigurations("app", databaseName);     // "app" is the file name of the properties file
        DataSourceFactory.loadDbConfigurations(configs);
    }


}
