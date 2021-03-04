package architecture;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import rdbms.RDBMS;
import shared.connectivity.thor.input.InputHandler;
import shared.connectivity.thor.response.Component;
import shared.connectivity.thor.response.GeneralArchitecture;
import shared.connectivity.thor.response.Response;
import shared.connectivity.thor.response.Table;
import tools.BasicFunctions;
import tools.Timer;
import tools.Timer.Type;
import dataStructure.ParseTreeNode;
import dataStructure.Query;
import dataStructure.SystemResult;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;


import components.*;

public class CommandInterface
{
	LexicalizedParser lexiParser;
	RDBMS db;
	Document tokens;
	Integer maxResults;

	Query query;
	public String feedback = "";

	public static final Boolean DEBUG_PRINTS = false;

	public static void main(String [] args) throws Exception
	{
		CommandInterface system = new CommandInterface();

		// Instantiate Thor's input Handler
        InputHandler inputHandler = new InputHandler();

		// Loop till the condition brakes from inside the loop
		while(true) {
			// Call Thor's read input
			inputHandler.readInput();

			// Read the parameters needed for the execution from the stdin.
			String query = inputHandler.getQuery();
			String schemaName = inputHandler.getSchemaName();
			system.maxResults = inputHandler.getResultsNumber();
			if (inputHandler.shutDownSystem())
				break;

			// Execute a #useDB command to load the schema
			system.executeCommand("#useDB " + schemaName);

			// Execute a #query command
			system.executeCommand("#query " + query);
		}
	}

	public CommandInterface() throws Exception
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

		if(command.startsWith("#useDB") && command.length() > 7)
		{
			db = new RDBMS(command.substring(7));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
			query = null;
		}
		else if(command.startsWith("#query") && command.length() > 7)
		{
			inputQuery(command.substring(7));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}
		else if(command.startsWith("#mapSchema") && command.length() > 11)
		{
			mapChoice(command.substring(11));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}
		else if(command.startsWith("#mapValue") && command.length() > 10)
		{
			mapValueChoice(command.substring(10));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}
		else if(command.startsWith("#general") && command.length() > 9)
		{
			setGeneral(command.substring(9));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}
		else if(command.startsWith("#specific") && command.length() > 10)
		{
			deleteSpecific(command.substring(10));
			feedback = FeedbackGenerator.feedbackGenerate(db.history, query);
		}

		System.out.println(FeedbackGenerator.feedbackGenerate(db.history, query));
	}

	public void inputQuery(String queryInput) throws Exception
	{
		// Create a timer
		Timer timer = new Timer(Type.WALL_CLOCK_TIME);
		List<SystemResult> results = new ArrayList<>();

		try {
			query = new Query(queryInput, db.schemaGraph);

			// ====================
			// Create a parser component
			components.StanfordNLParser.parse(query, lexiParser);

			// Debug Prints
			if (DEBUG_PRINTS) {
				System.out.println("BEFORE MAPPER");
				System.out.println(query.parseTree.toString());
			}
			// ------------

			// ====================
			// Create a Node Mapper
			components.NodeMapper.phraseProcess(query, db, tokens);


			// Debug Prints
			if (DEBUG_PRINTS) {
				System.out.println("AFTER MAPPER");
				System.out.println(query.parseTree.toString());
			}
			// -----------

			// ====================
			// Create Tree Structure Adjustor
			components.EntityResolution.entityResolute(query);
			components.TreeStructureAdjustor.treeStructureAdjust(query, db);


			// ====================
			// Create a Translator
			components.Explainer.explain(query);
			results = components.SQLTranslator.translate(query, db);

			// Prune the results if more the maxResults
			if (results == null || results.size() == 0) {
				System.out.println("WE COULD NOT PRODUCE A GOOD RESULTS");
            }
            // Let feedbak generator generate the output
            // else if (results != null) {
			// 	if (results.size() > this.maxResults)
			// 		results = results.subList(0, this.maxResults);

			// 	System.out.println("THE RESULTS\n");
			// 	SystemResult.print(results);
			// }
		}
		catch(Exception e) {
			System.err.println("[ERR] Exception ocurred while executing : " + queryInput);
			e.printStackTrace();
		}
	}



	public void mapChoice(String choiceInput) throws Exception
	{
		String [] commands = choiceInput.split(" ");
		if(commands.length == 2)
		{
			int wordOrder = Integer.parseInt(commands[0]);
			int schemaChoice = Integer.parseInt(commands[1]);
			ParseTreeNode node = query.parseTree.searchNodeByOrder(wordOrder);
			node.choice = schemaChoice;
		}

		components.EntityResolution.entityResolute(query);
		components.TreeStructureAdjustor.treeStructureAdjust(query, db);
		components.Explainer.explain(query);
		components.SQLTranslator.translate(query, db);
	}

	public void mapValueChoice(String choiceInput) throws Exception
	{
		String [] commands = choiceInput.split(" ");
		if(commands.length >= 2)
		{
			int wordOrder = Integer.parseInt(commands[0]);
			int valueChoice = Integer.parseInt(commands[1]);
			ParseTreeNode node = query.parseTree.searchNodeByOrder(wordOrder);

			node.mappedElements.get(node.choice).choice = valueChoice;
		}

		components.EntityResolution.entityResolute(query);
		components.TreeStructureAdjustor.treeStructureAdjust(query, db);
		components.Explainer.explain(query);
		components.SQLTranslator.translate(query, db);
	}

	public void setGeneral(String choiceInput)
	{
		if(BasicFunctions.isNumeric(choiceInput))
		{
			query.queryTreeID = Integer.parseInt(choiceInput);
			components.SQLTranslator.translate(query, db);
		}
	}

	private void deleteSpecific(String deleteID)
	{
		if(BasicFunctions.isNumeric(deleteID))
		{
			components.NodeInserter.deleteNode(query.queryTree, query.NLSentences.get(query.queryTreeID), Integer.parseInt(deleteID));
			components.SQLTranslator.translate(query, db);
		}
	}
}