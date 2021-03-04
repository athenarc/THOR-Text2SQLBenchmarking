package discover;

import shared.connectivity.thor.input.InputHandler;
import shared.connectivity.thor.response.Component;
import shared.connectivity.thor.response.GeneralArchitecture;
import shared.connectivity.thor.response.Response;
import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DataSourceFactory;
import shared.database.model.graph.SchemaGraph;
import shared.database.model.DatabaseType;
import shared.database.model.SQLDatabase;
import shared.util.Pair;
import shared.util.Timer;
import shared.util.Timer.Type;
import discover.model.TupleSet;
import discover.model.TupleSetGraph;
import discover.model.execution.ExecutionPlan;
import discover.model.FreeTupleSet;
import discover.model.JoiningNetworkOfTupleSets;
import discover.model.OverloadedTuple;
import discover.components.TupleSetPostProcessor;
import discover.components.CandidateNetworksGenerator;
import discover.components.MasterIndex;
import discover.components.Parser;
import discover.components.PlanGenerator;
import discover.components.executors.PlanExecutor;
import discover.exceptions.ShutdownHook;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DiscoverApplication {

    // Parameters
    String query;
    String schemaName;
    DatabaseType databaseType;
    private int maxTuples;
    int maxNetworkSize;

    // Database Variables
    HashMap<String, Pair<SQLDatabase, SchemaGraph>> storedDatabases = new HashMap<>();
    SQLDatabase database;
    SchemaGraph schemaGraph;

    // Execution Variables
    List<String> keywords;
    List<TupleSet> basicTupleSets;
    List<TupleSet> keywordSubsetTupleSets;
    TupleSetGraph tupleSetGraph;
    List<JoiningNetworkOfTupleSets> candidateNetworks;
    ExecutionPlan executionPlan;
    List<OverloadedTuple> finalResults = new ArrayList<>();


    // Static Variables
    public static final Boolean DEBUG_PRINTS = false;
    public static Boolean USE_VALUE_CONST = true;
    public static final Boolean USE_INTERMEDIATE_RESULTS = false;
    public static Boolean USE_VIEWS = false;

    public static void main(String[] args) {
        // Assign the shutdown hook
        Runtime.getRuntime().addShutdownHook(new ShutdownHook());

        // Fill the configurations using the "app.properties" file
        PropertiesSingleton.loadPropertiesFile("app");

        // Create an instate of the discover App
        DiscoverApplication discoverApp = new DiscoverApplication();
        Timer timer = new Timer(Type.WALL_CLOCK_TIME), totalTimer = new Timer(Type.WALL_CLOCK_TIME);
        discoverApp.maxNetworkSize = 5;

        // Read and store all known databases for THOR
        // discoverApp.readAndStoreDatabases(new String[]{"mysql.IMDB", "mysql.YELP", "mysql.MAS"});

        // Instantiate Thor's input Handler
        InputHandler inputHandler = new InputHandler();

        // Loop till the condition brakes from inside the loop
        while(true) {

            discoverApp.resetVars();

            // Call Thor's read input
            inputHandler.readInput();
            totalTimer.start();

            // Read the parameters needed for the execution from the stdin.
            discoverApp.query = inputHandler.getQuery();
            discoverApp.schemaName = inputHandler.getDatabaseName();
            discoverApp.databaseType = inputHandler.getDatabaseType();
            discoverApp.maxTuples = inputHandler.getResultsPerInterpretation();
            if (inputHandler.shutDownSystem())
                break;

            // Create all the Component objects that mirror the
            // actual components of discover and will be displayed in THOR.
            GeneralArchitecture genArch = new GeneralArchitecture();
            Component mIndex = new Component("MasterIndex");
            Component pProcessor = new Component("TupleSetPostProcessor");
            Component cnGenerator = new Component("Candidate Network Generator");
            Component pGenerator = new Component("Plan Generator");
            Component pExecutor = new Component("Plan Executor");

            try {
                // Create Database and Schema Graph automatically
                discoverApp.getDatabaseAndSchemaGraph();

                // Parse query and crete BasicTupleSets
                discoverApp.parseQueryAndCreateBasicTupleSets(timer, mIndex, genArch);

                // Use the Basic Tuple Sets to generate all possible combinations
                // of Tuple Sets containing more than one keyword on each TupleSet.
                discoverApp.generateTupleSetCombinations(timer, pProcessor);

                // Create the candidate Networks
                discoverApp.generateCandidateNetworks(timer, cnGenerator, genArch);

                // Generate the execution Plan
                discoverApp.generateExecutionPlan(timer, pGenerator);

                // Execute the Plan generated above
                discoverApp.runExecutionPlan(timer, pExecutor, genArch);
            }
            catch (Exception e) {
                System.err.println("[ERR] Exception ocurred while executing : " + discoverApp.query);
                e.printStackTrace();
            }


            // =================
            // Connect with THOR
            // =================

            // Create the architecture of the system.
            // mIndex.connectWith(pProcessor, "Basic tuple sets");
            // pProcessor.connectWith(cnGenerator, "Non-empty tuple sets");
            // cnGenerator.connectWith(pGenerator, "Candidate networks");
            // pGenerator.connectWith(pExecutor, "Execution plan");

            // // Create a response and sent it to THOR
            // Response<OverloadedTuple> response = new Response<OverloadedTuple>(
            //     "discover",
            //     "Discover",
            //     genArch,
            //     Arrays.asList(mIndex, pProcessor, cnGenerator, pGenerator, pExecutor),
            //     discoverApp.finalResults
            // );
            // response.sendToTHOR();

            System.err.println("TOTAL TIME: " + totalTimer.stop());
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
            String name = dbName.split("\\.")[1];
            String type = dbName.split("\\.")[0];

            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(name, DatabaseType.getTypeFromString(type) );

            // Create PK-FK Relationship Graph.
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillDirected(database.getTables(), database.getFKConstrains());

            // Store the db.
            this.storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }


    /**
     * Fills the Database and the Schema Graph variables
     *
     */
    void getDatabaseAndSchemaGraph() {
        // If we have stored the database then dont read it again.
        // Else read the db and store it.
        if (this.storedDatabases != null && this.storedDatabases.containsKey(schemaName)) {
            Pair<SQLDatabase, SchemaGraph> dbPair = this.storedDatabases.get(schemaName);

            // Change the Datasource Object to reflect the new database name
            DataSourceFactory.loadConnectionProperties(this.schemaName, this.databaseType);

            this.database = dbPair.getLeft();
            this.schemaGraph = dbPair.getRight();
        }
        else {
            // Automatically get the info of database.
            this.database = SQLDatabase.InstantiateDatabase(this.schemaName, this.databaseType);

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
     * Parse the query to keywords and create the Basic Tuple Sets Using the Master Index Component
     *
     * @param timer
     * @param mIndex
     */
    void parseQueryAndCreateBasicTupleSets(Timer timer, Component mIndex, GeneralArchitecture genArch) {
        // Parse the query into keywords and print them.
        this.keywords = Parser.whitespaceTokenizer(query);

        // Create the MasterIndex instance and print the basic tuple sets.
        timer.start();
        MasterIndex masterIndex = new MasterIndex(keywords, database);
        this.basicTupleSets = masterIndex.generateBasicTupleSets();

        // Set stats
        mIndex.setTime(timer.stop());
        mIndex.addComponentInfo(masterIndex.getStatistics());
        genArch.setEntityMapperOutput(masterIndex.getRelationsFromMappings());

        // DEBUG PRINTS
        if (DEBUG_PRINTS) {
            System.out.println("Keywords: " + keywords + "\n");
            System.out.println("MASTER INDEX TIMER " + mIndex.getTime());
            System.out.println("=====================================\n");
            System.out.println("BASIC TUPLE SETS\n");
            for (TupleSet set : basicTupleSets) {
                System.out.println(set.toAbbreviation());
                if (set.getSize() < 50)
                    set.print();
                System.out.println();
            }
            masterIndex.printStats();
            System.out.println();
        }
    }


    /**
     * Use the Post Processor to generate all possible combinations of TuplesSets
     * that contain more than one keyword. The PostProcessor uses the BasicTuple sets
     * produces by the masterIndex component
     *
     * @param timer
     * @param pProcessor
     */
    void generateTupleSetCombinations(Timer timer, Component pProcessor) {
        timer.start(); // Start the timer.

        // Create postProcessor Instance and print all TupleSets for all subsets of K and all relations
        TupleSetPostProcessor postProcessor = new TupleSetPostProcessor(this.keywords, this.basicTupleSets);
        this.keywordSubsetTupleSets = postProcessor.generateKeywordSubsetsTupleSets();

        // Add stats to the component representation.
        pProcessor.setTime(timer.stop());
        pProcessor.addComponentInfo(postProcessor.getStatistics());

        if (DEBUG_PRINTS) {
            System.out.println("=====================================\n");
            System.out.println("TUPLE SET POST PROCESSOR");
            System.out.println("Keywords subsets Ri^K for each relation\n");
            for (TupleSet set : keywordSubsetTupleSets) {
                System.out.println(set.toAbbreviation());
                if (set.getSize() < 50)
                    set.print();
                System.out.println();
            }
            postProcessor.printStats();
        }
    }


    /**
     * Create the TupleSet Graph
     */
    void createTupleSetGraph() {
        // Create a list with all the free and non-free tuple sets.
        List<TupleSet> freeAndNonFreeTupleSets = new ArrayList<TupleSet>();
        freeAndNonFreeTupleSets.addAll(this.keywordSubsetTupleSets);
        freeAndNonFreeTupleSets.addAll(FreeTupleSet.getFreeTupleSets(this.database.getTables()));

        // Create the tupleSets graph.
        this.tupleSetGraph = new TupleSetGraph();
        tupleSetGraph.fill(freeAndNonFreeTupleSets, schemaGraph);

        // Debug Prints
        if (DEBUG_PRINTS) {
            System.out.println("\n=====================================\n");
            System.out.println("Tuple Set Graph\n");
            System.out.println("\n=====================================\n");
            System.out.println(tupleSetGraph);
        }
    }

    /**
     * Generate the Execution Plan using the PlaneGenerator component
     *
     * @param timer
     * @param pGenerator
     */
    void generateExecutionPlan(Timer timer, Component pGenerator) {
        timer.start(); // Start the timer.

        // Initialize the plan generator and generate the execution plan.
        PlanGenerator planGenerator = new PlanGenerator();

        // Create an execution plan containing intermediate results or not.
        if (USE_INTERMEDIATE_RESULTS)
            this.executionPlan = planGenerator.generateExecutionPlan(this.candidateNetworks);
        else
            this.executionPlan = planGenerator.generateExecutionPlan_CN_ONLY(this.candidateNetworks);

        // Set statistics
        pGenerator.setTime(timer.stop());
        pGenerator.addComponentInfo(planGenerator.getStatistics());

        // Debug Prints
        if (DEBUG_PRINTS) {
            System.out.println("=====================================\n");
            System.out.println("ExecutionPlan:\n");
            System.out.println("=====================================\n");
            System.out.println(this.executionPlan);
            planGenerator.printStats();
            System.out.println();
        }
    }


    /**
     * Generate the Candidate Networks using the CandidateNetworkGenerator component.
     *
     * @param timer
     * @param cnGenerator
     */
    void generateCandidateNetworks(Timer timer, Component cnGenerator, GeneralArchitecture genArch) {
        timer.start();

        // Create the TupleSet graph
        this.createTupleSetGraph();

        // ================================
        // Initialize the candidate networks generator component.
        CandidateNetworksGenerator candidateNetworksGenerator = new CandidateNetworksGenerator(
            this.keywordSubsetTupleSets, this.tupleSetGraph,
            this.keywords, this.maxNetworkSize
        );

        // Generate the networks.
        candidateNetworksGenerator.generateCandidateNetworks();
        this.candidateNetworks = candidateNetworksGenerator.getCandidateNetworks();

        // Set statistics
        cnGenerator.setTime(timer.stop());
        cnGenerator.addComponentInfo(candidateNetworksGenerator.getStatistics());
        genArch.setInterpretationGeneratorOutput(candidateNetworks.size());

        // Print the candidate networks.
        if (DEBUG_PRINTS) {
            System.out.println("=====================================\n");
            System.out.println("Candidate Network Generator\n");
            System.out.println("=====================================\n");

            System.out.println("Candidate Networks\n");
            for (JoiningNetworkOfTupleSets jnts : this.candidateNetworks) {
                System.out.println(jnts + "\n");
            }

            candidateNetworksGenerator.printStats();
            System.out.println();
        }
    }


    /**
     * Run the Execution Plan produced by the PlanExecutor component.
     *
     * @param timer
     * @param pExecutor
     */
    void runExecutionPlan(Timer timer, Component pExecutor, GeneralArchitecture genArch) {
        timer.start(); // Start the timer.

        // Create the plan executor and execute the plan.
        PlanExecutor planExecutor = new PlanExecutor(this.schemaGraph, this.database, this.maxTuples);
        planExecutor.execute(this.executionPlan, this.keywordSubsetTupleSets);

        // Set statistics
        pExecutor.setTime(timer.stop());
        pExecutor.addComponentInfo(planExecutor.getStatistics());
        genArch.setTranslatorAndExecutorOutput(this.executionPlan.getAssignments().size()); // every assignment is an sql query

        // Get results
        this.finalResults = planExecutor.getResults();

        System.out.println("==============\n");
        System.out.println("Plan Executor:\n");
        System.out.println("==============\n");
        planExecutor.printStats(executionPlan);
        System.out.println("[INFO] Resutls (" + planExecutor.getAllResults().size()  + ") :" );
        for (OverloadedTuple ot: this.finalResults)
            System.out.println(ot);
    }


    /** Resets the variables  */
    public void resetVars() {
        this.keywords = null;
        this.basicTupleSets = null;
        this.keywordSubsetTupleSets = null;
        this.tupleSetGraph = null;
        this.candidateNetworks = null;
        this.executionPlan = null;
        this.finalResults = new ArrayList<>();
    }
}
