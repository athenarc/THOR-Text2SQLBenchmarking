package expressq2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import expressq2.components.Parser;
import expressq2.components.QueryAnalyzer;
import expressq2.components.QueryInterpreter;
import expressq2.components.QueryTranslator;
import expressq2.components.SQLQueryExecutor;
import expressq2.model.AnnotatedQuery;
import expressq2.model.Keyword;
import expressq2.model.OverloadedTuple;
import expressq2.model.OverloadedTupleList;
import expressq2.model.QueryPattern;
import expressq2.model.SQLQuery;

import shared.database.connectivity.DatabaseConfigurations;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.ORMSchemaGraph;
import shared.util.OrderedList;
import shared.util.Pair;
import shared.util.Timer;
import shared.util.Timer.Type;
import shared.connectivity.thor.input.InputHandler;
import shared.connectivity.thor.response.Component;
import shared.connectivity.thor.response.GeneralArchitecture;
import shared.connectivity.thor.response.Response;
import shared.connectivity.thor.response.Table;


public class ExpressQ2Application {
    // Parameters
    String query;
    String schemaName;
    int maxTuples;
    int maxPatterns = 5;   // The maximum number of QueryInterpretations executed and returned to the user.

    // Database Variables
    HashMap<String, Pair<SQLDatabase, ORMSchemaGraph>> storedDatabases = new HashMap<>();
    SQLDatabase database;
    ORMSchemaGraph schemaGraph;

    // Execution Variables
    private List<Keyword> keywords;
    private List<AnnotatedQuery> annotatedQueries;
    private OrderedList<QueryPattern, Double> queryPatternList;
    private List<OverloadedTuple> finalResults = new ArrayList<>();


    // Static Variables
    static final Boolean DEBUG_PRINTS = false;


    public static void main(String[] args) {
        // Load the database configurations from the configurations file.
        DatabaseConfigurations.fill("app");

        // Create an instance of the ExpressQ2 app
        ExpressQ2Application expressQ2App = new ExpressQ2Application();

        // Instantiate Thor's input Handler
        InputHandler inputHandler = new InputHandler();

        // Read all THOR's databases
        // expressQ2App.readAndStoreDatabases(new String[]{"IMDB", "MAS", "YELP"});

        // Create the timers
        Timer timer = new Timer(Type.WALL_CLOCK_TIME), totalTimer = new Timer(Type.WALL_CLOCK_TIME);

        // Loop till the condition brakes from inside the loop
        while(true) {
            totalTimer.start();
            expressQ2App.resetVars();

            // Call Thor's read input
            inputHandler.readInput();

            // Read the parameters needed for the execution from the stdin.
            expressQ2App.query = inputHandler.getQuery();
            expressQ2App.schemaName = inputHandler.getSchemaName();
            expressQ2App.maxTuples = inputHandler.getResultsNumber();
            if (inputHandler.shutDownSystem())
                break;

            // Create all the Component objects that mirror the
            // actual components of discover and will be displayed in THOR.
            GeneralArchitecture genArch = new GeneralArchitecture();
            Component qAnalyzer = new Component("Query Analyzer");
            Component qInterpreter = new Component("Query Interpreter");
            Component translator = new Component("Translator");
            Component executor = new Component("Executor");

            try {
                // Get the database and the Schema Graph
                expressQ2App.getDatabaseAndORMSchemaGraph(timer);

                // Parse and Annotate the query
                expressQ2App.parseAndAnnotateQuery(timer, qAnalyzer, genArch);

                // Interpret the annotates Queries into Query Patterns
                expressQ2App.createQueryPatters(timer, qInterpreter, genArch);

                // Translate and execute the query Patterns
                expressQ2App.translateAndExecuteQueryPatterns(timer, translator, executor, genArch);
            }
            catch (Exception e) {
                System.err.println("[ERR] Exception ocurred while executing : " + expressQ2App.query);
                e.printStackTrace();
            }


            // =================
            // Connect with THOR
            // =================

            // Create the architecture of the system.
            // qAnalyzer.connectWith(qInterpreter, "Annotated Query");
            // qInterpreter.connectWith(translator, "Query Pattern");
            // translator.connectWith(executor, "SQLQuery");

            // // Create a response and sent it to THOR
            // Response<OverloadedTuple> response = new Response<OverloadedTuple>(
            //     "expressq",
            //     "ExpressQ",
            //     genArch,
            //     Arrays.asList(qAnalyzer, qInterpreter, translator, executor),
            //     expressQ2App.finalResults
            // );
            // response.sendToTHOR();
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
            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(dbName);

            // Create PK-FK Relationship Graph.
            ORMSchemaGraph schemaGraph = new ORMSchemaGraph();
            schemaGraph.fill(database.getTables(), database.getFKConstrains());

            // Store the db.
            this.storedDatabases.put(dbName, new Pair<>(database, schemaGraph));
        }
    }



    /**
     * Get the database and schema Graph automatically
     *
     * @param timer
     */
    public void getDatabaseAndORMSchemaGraph(Timer timer) {
        timer.start();

        if (this.storedDatabases.containsKey(this.schemaName)) {
            DatabaseConfigurations.useDatabase(schemaName);

            // Get the cached database and ORM graph
            Pair<SQLDatabase, ORMSchemaGraph> dbPair = this.storedDatabases.get(this.schemaName);
            this.database = dbPair.getLeft();
            this.schemaGraph = dbPair.getRight();
        }
        else {
            // Create a database object on the database.
            this.database = SQLDatabase.InstantiateDatabase(schemaName);

            // Create an ORM schema Graph.
            this.schemaGraph = new ORMSchemaGraph();
            schemaGraph.fill(database.getTables(), database.getFKConstrains());

            // Put the database in the storedDatabases
            this.storedDatabases.put(this.schemaName, new Pair<>(this.database, this.schemaGraph));
        }

        // Debug Prints
        if (DEBUG_PRINTS) {
            System.out.println("Database: \n" + this.database + "\n\nORMSchemaGraph: \n" + this.schemaGraph);
            System.out.println("[INFO] Recognized Database & Created Graph in: " + timer.stop());
        }
    }


    /**
     * Parse and Annotate the Query got from the user.
     *
     * @param timer
     * @param qAnalyzer
     */
    public void parseAndAnnotateQuery(Timer timer, Component qAnalyzer, GeneralArchitecture genArch) {
        // Parse the query into keywords and print them.
        this.keywords = Parser.whitespaceTokenizer(query);

        // Analyze the query and annotate it.
        timer.start();
        QueryAnalyzer queryAnalyzer = new QueryAnalyzer(this.database, this.keywords, this.schemaGraph);  // The Query Analyzer
        this.annotatedQueries = queryAnalyzer.createAnnotatedQueries();                    // The Annotated Queries

        // Check if the Annotated Queries are valid.
        List<AnnotatedQuery> queriesToRemove = new ArrayList<>();
        for (AnnotatedQuery annotatedQuery: this.annotatedQueries) {
            // Store the non valid Queries to a List.
            if (!Parser.checkAnnotatedQueryValidity(annotatedQuery))
                queriesToRemove.add(annotatedQuery);
        }

        // Remove the non valid Quires.
        annotatedQueries.removeAll(queriesToRemove);

        // Add Query Analyzer Stats
        List<Table.Row> rows = new ArrayList<>();
        rows.add(new Table.Row(Arrays.asList("Grammatically wrong Annotated Queries", Integer.toString(queriesToRemove.size()))));
        rows.add(new Table.Row(Arrays.asList("Grammatically correct Annotated Queries", Integer.toString(annotatedQueries.size()))));

        // Update Components Time and Stats
        qAnalyzer.setTime(timer.stop());
        qAnalyzer.addComponentInfo(queryAnalyzer.getStatistics());
        qAnalyzer.addComponentInfo(new Table(rows));
        genArch.setEntityMapperOutput(queryAnalyzer.getRelationsNumFromMappings());
        genArch.mapWithEntityMapper(qAnalyzer);

        if (DEBUG_PRINTS) {
            System.out.println("[INFO] Keywords: " + keywords);
            System.out.println("[INFO] Analyze query in: " + timer);
        }
    }

    /**
     * Create the IntermediateResults Interpretations (QueryGraphs) using the QueryInterpreter Component
     *
     * @param timer
     * @param qInterpreter
     */
    public void createQueryPatters(Timer timer, Component qInterpreter, GeneralArchitecture genArch) {
        timer.start();
        List<QueryPattern> allQueryGraphs = new ArrayList<>();

        // Create the Query Patterns
        this.queryPatternList = new OrderedList<>(maxPatterns);

        if (DEBUG_PRINTS)
            System.out.println("[INFO] The annotate Queries Interpretations\n");

        // Loop each annotated Query and interpret it to query patterns.
        int num = 0;
        for (AnnotatedQuery annotatedQuery: annotatedQueries) {
            Timer innerTimer = new Timer();

            // Create all the Query Patterns extracted by the query.
            innerTimer.start();
            List<QueryPattern> pList = QueryInterpreter.interpret(annotatedQuery, this.schemaGraph);
            innerTimer.stop();
            allQueryGraphs.addAll(pList);

            // Get the score from each Query and Add them in the Queries List based on that score.
            Double score = pList.get(0).getScore();
            queryPatternList.addElement(pList.get(0), score);

            if (DEBUG_PRINTS) {
                System.out.println("Q" + num++ + ":" + annotatedQuery.toString() + "\n");
                System.out.println("Pattern:\n" + pList.get(0) + "[RES] Score: " + score + "\n");
                System.out.println("[INFO] Interpreted in: " + innerTimer + "\n------------------------------\n\n");
            }

            // for (QueryPattern p: pList) {
            //     double score = p.getScore();
            //     if (DEBUG_PRINTS)
            //         System.out.println("Pattern:\n" + p + "[RES] Score: " + score + "\n");
            //     queryPatternList.addElement(p, score);
            // }
        }

        // Set statistics
        qInterpreter.setTime(timer.stop());
        qInterpreter.addComponentInfo(QueryInterpreter.getStatistics(allQueryGraphs));
        genArch.setInterpretationGeneratorOutput(annotatedQueries.size());
        genArch.mapWithInterpretationGenerator(qInterpreter);
    }


    /**
     * Translate and execute the QueryPatterns.
     *
     * @param timer
     * @param translator
     * @param executor
     */
    public void translateAndExecuteQueryPatterns(Timer timer, Component translator, Component executor, GeneralArchitecture genArch) {
        // Translate, Execute and print the top K QueryPatterns.
        if (DEBUG_PRINTS)
            System.out.println("\n+++++++++++TOP " + maxPatterns + " Queries+++++++++++++++++\n");

        Double translationTime = 0D, executionTime = 0D;
        Integer sqlQueriesCreated = 0;
        for (Pair<Double, QueryPattern> pattern: queryPatternList.getElementsWithScore()) {
            Timer translationTimer = new Timer(), executionTimer = new Timer();  // Timers.
            QueryPattern p = pattern.getRight();                                 // The Query Pattern.

            translationTimer.start();
            SQLQuery sqlQuery = QueryTranslator.translateQueryPattern(database, p, schemaGraph);
            sqlQueriesCreated++;
            translationTime += translationTimer.stop();

            if (DEBUG_PRINTS) {
                System.out.println("Graph:\n" + p + "[RES] Query:\n" + sqlQuery + "\n[RES] Score: " + pattern.getLeft() + "\n");
                System.out.println("[INFO] Translated to slq in: " + translationTimer.stop());
            }

            // NOTE : I Have removed the select distinct subQuery on relationship relations cause it takes TOOOO much time.
            executionTimer.start();
            OverloadedTupleList results = SQLQueryExecutor.executeQuery(sqlQuery, database, pattern.getLeft());
            executionTime += executionTimer.stop();

            if (DEBUG_PRINTS) {
                System.out.println("[INFO] Executed along an SQLEngine in: " + executionTimer);
                System.out.println();
                results.print(true);
            }

            // Keep the results
            this.finalResults.addAll(results.getTupleList());
        }

        // Set statistics
        translator.setTime(translationTime);
        translator.addComponentInfo(new Table(Arrays.asList(new Table.Row(Arrays.asList("SQL Queries created", sqlQueriesCreated.toString())))));

        executor.setTime(executionTime);
        executor.addComponentInfo(new Table(Arrays.asList(new Table.Row(Arrays.asList("Total Results", Integer.toString(this.finalResults.size()))))));
        genArch.setTranslatorAndExecutorOutput(queryPatternList.getElements().size());
        genArch.mapWithTranslatorAndExecutor(translator, executor);

        // Prune the results to maxTuples
        if (this.finalResults.size() > this.maxTuples)
            this.finalResults = this.finalResults.subList(0, this.maxTuples);

        // Print the results
        System.out.println("[INFO] The Results");
        for (OverloadedTuple tup: this.finalResults)
            System.out.println(tup.toString());
    }

    /** Reset the variables before each query */
    public void resetVars() {
        this.keywords = null;
        this.annotatedQueries = null;
        this.queryPatternList = null;
        this.finalResults = new ArrayList<>();
    }
}


/* IMDB Queries to Keywords */
// String query = "movie actor \"Matt Damon\"";         // Multiple Annotated Queries + Multiple Graphs from different Paths!
// String query = "Count movie actor \"Angelina Jolie\" \"Brad Pitt\" ";
// String query = "COUNT release_year \"Dead Poets Society\"";
// String query = "Director movie \"Catch me if you can\"";
// String query = "Count actor movie \"Grumpier Old Men\"";
// String query = "movie director \"Alfred Hitchcock\" actor \"Brad Pitt\" ";
// String query = "Count movie writer \"Matt Damon\"";
// String query = "Count movie writer \"Woody Allen\"  producer \"Woody Allen\"";
// String query = "company movie \"Juno\"";
// String query = "COUNT movie GroupBy actor";

/* MAS Queries */
// String query = "COUNT journal publication \"H. V. Jagadish\"";
// String query = "publication author ioannidis";

/* University Queries */
// String query = "George COUNT Code";                        // Multiple Annotated Queries!
// String query = "Green COUNT Code";                         // Multiple Annotated Queries + Multiple Graphs from Disambiguation!
// String query = "Green COUNT Code";                         // Multiple Annotated Queries + Multiple Graphs from different Paths!
// String query = "Student George SUM Credit";
// String query = "SUM price JAVA";
