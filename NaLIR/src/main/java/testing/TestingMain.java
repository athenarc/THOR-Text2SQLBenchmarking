package testing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.AbstractMap.SimpleEntry;

import rdbms.RDBMS;
import tools.Timer;
import tools.Timer.Type;
import dataStructure.Query;
import dataStructure.SystemResult;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;

import architecture.CommandInterface;
import components.*;


public class TestingMain {

	static final String DEBUG_FILE = "NaLIRDebug.txt";
	static final String QUERY_FILE = "queries";

	LexicalizedParser lexiParser;
	RDBMS db;
	Document tokens;
	Integer maxResults;

	Query query;
	public String feedback = "";

	// Infos
	Info answer;

	public static void main(String[] args) throws Exception
	{
		// Redirect all output to file
		PrintStream o = new PrintStream(new File(DEBUG_FILE));
		PrintStream originalOut = System.out;

		// Initialize variables
		TestingMain system = new TestingMain();
		List<String> queries = getQueryList(QUERY_FILE);
		system.maxResults = 10;
		InfoWriter.init();

		// A delay to open the visual vm
        Scanner scanner = new Scanner(System.in);
        System.out.println("Press enter to start:");
        scanner.nextLine();

        // Loop till the condition brakes from inside the loop
        for (String overloadedQuery: queries) {
			String query = overloadedQuery.split(";")[0];
			String schemaName = overloadedQuery.split(";")[1];
			system.answer = new Info();

			// Execute a #useDB command to load the schema
			system.executeCommand("#useDB " + schemaName);
			System.out.println("[INFO] Executing: " + query);

			try {
				// Swap to file print
				System.setOut(o);

				// Execute a #query command
				system.executeCommand("#query " + query);

				// Swap back to original output stream
                System.setOut(originalOut);

            } catch (Exception e) {
                System.out.println("[ERR] Failed ...");
                e.printStackTrace();
            }
            System.out.println("\tDone...");

			// Write the stats
			// InfoWriter.toFile(system.answer);
		}
	}

	public TestingMain() throws Exception
	{
		lexiParser = LexicalizedParser.loadModel("edu/stanford/nlp/models/lexparser/englishPCFG.ser.gz");

		// Parse it with the DOM Parser
		InputStream is = ClassLoader.getSystemResourceAsStream("zfiles/tokens.xml");
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
		tokens = dBuilder.parse(is);
	}

	public void executeCommand(String command) throws Exception
	{
		// System.out.println("command: " + command);

		if(command.startsWith("#useDB") && command.length() > 7)
		{
			db = new RDBMS(command.substring(7));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
			query = null;
		}
		else if(command.startsWith("#query") && command.length() > 7)
		{
			inputQuery(command.substring(7));
			// feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}

		if (CommandInterface.DEBUG_PRINTS == true)
			System.out.println(FeedbackGenerator.feedbackGenerate(db.history, query));
	}

	public void inputQuery(String queryInput) throws Exception
	{
		// Create a timer + Create the query
		Timer timer = new Timer(Type.WALL_CLOCK_TIME);
		query = new Query(queryInput, db.schemaGraph);
		this.answer.query = queryInput;

		// System.out.println("QUERY: " + queryInput);

		// ====================
		// Create a parser component
		PerformanceMonitor monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
		timer.start();
		components.StanfordNLParser.parse(query, lexiParser);

			// Set statistics
			this.answer.timerPerComponent.add(timer.stop());
			this.answer.cpuParser = monitor.calcCpuPer();
			this.answer.memParser = monitor.calcMemUsage();

		// Debug Prints
		if (CommandInterface.DEBUG_PRINTS) {
			System.out.println("BEFORE MAPPER");
			System.out.println(query.parseTree.toString());
		}
		// ------------

		// ====================
		// Create a Node Mapper
		monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
		timer.start();
		components.NodeMapper.phraseProcess(query, db, tokens);

			// Set statistics
			this.answer.timerPerComponent.add(timer.stop());
			this.answer.mappingsPerKW = NodeMapper.mappingsPerKW;
			this.answer.sqlIO = 0;
			for (SimpleEntry<Integer, Integer> entry: NodeMapper.mappingsPerKW.values()) {
				this.answer.sqlIO += entry.getValue();
			}
			this.answer.cpuMapper = monitor.calcCpuPer();
			this.answer.memMapper = monitor.calcMemUsage();

		// Debug Prints
		if (CommandInterface.DEBUG_PRINTS) {
			System.out.println("AFTER MAPPER");
			System.out.println(query.parseTree.toString());
		}
		// -----------

		// ====================
		// Create Tree Structure Adjustor

		monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
		timer.start();
		components.EntityResolution.entityResolute(query);
		components.TreeStructureAdjustor.treeStructureAdjust(query, db);

			// Set statistics
			this.answer.timerPerComponent.add(timer.stop());
			this.answer.numOfIRepresentations = query.adjustingTrees.size();
			this.answer.cpuEntRes = monitor.calcCpuPer();
			this.answer.memEntRes = monitor.calcMemUsage();

		// ====================
		// Create a Translator
		monitor = PerformanceMonitor.builder().startCpuMonitor().startMemMonitor();
		timer.start();
		components.Explainer.explain(query);
		List<SystemResult> results = components.SQLTranslator.translate(query, db);

			// Set statistics
			this.answer.timerPerComponent.add(timer.stop());
			this.answer.cpuTranslator = monitor.calcCpuPer();
			this.answer.memTranslator = monitor.calcMemUsage();


		// Prune the results if more the maxResults
		if (results == null || results.size() == 0) {
			// System.out.println("WE COULD NOT PRODUCE A GOOD RESULTS");
		} else if (results != null) {
			if (results.size() > this.maxResults)
				results = results.subList(0, this.maxResults);

			// System.out.println("THE RESULTS\n");
			// SystemResult.print(results);
			this.answer.numOfResults = results.size();
		}

		// Print separation Line - add query stats
		// System.out.println("\n^^^^^^^^^^^^^^^^^^\n^^^^^^^^^^^^^^^^^^\n\n");
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
}