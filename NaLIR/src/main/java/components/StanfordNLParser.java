package components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import architecture.CommandInterface;
import dataStructure.ParseTree;
import dataStructure.ParseTreeNode;
import dataStructure.PosTags;
import dataStructure.Query;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.parser.lexparser.LexicalizedParser;
import edu.stanford.nlp.trees.GrammaticalStructure;
import edu.stanford.nlp.trees.GrammaticalStructureFactory;
import edu.stanford.nlp.trees.PennTreebankLanguagePack;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreePrint;
import edu.stanford.nlp.trees.TreebankLanguagePack;
import edu.stanford.nlp.trees.TypedDependency;
import shared.connectivity.thor.response.Table;

public class StanfordNLParser 
{
	public static void parse(Query query, LexicalizedParser lexiParser)
	{
		StanfordParse(query, lexiParser); 
		buildTree(query); 		
		fixConj(query);		
	}
	
	public static void StanfordParse(Query query, LexicalizedParser lexiParser)
	{
		List<CoreLabel> rawWords = SentenceUtils.toCoreLabelList(query.sentence.outputWords); // use Stanford parser to parse a sentence; 
    	Tree parse = lexiParser.apply(rawWords); 
    	TreebankLanguagePack tlp = new PennTreebankLanguagePack();
    	GrammaticalStructureFactory gsf = tlp.grammaticalStructureFactory();
    	GrammaticalStructure gs = gsf.newGrammaticalStructure(parse);
    	List<TypedDependency> dependencyList = gs.typedDependencies(false);
    	
    	Iterator<Tree> iter = parse.iterator(); 
    	ArrayList<String> allNodes = new ArrayList<String>(); // allNodes in format root [num]\n S [num]\n...
    	while(iter.hasNext())
    	{
    		allNodes.add(iter.next().nodeString()); 
    	}
    	
    	ArrayList<String []> allWords = new ArrayList<String []>(); // allWords in format word, pos; 
    	String [] word = {"ROOT", "ROOT"}; 
		allWords.add(word); 
    	for(int i = 0; i < allNodes.size(); i++)
    	{
    		if(query.sentence.wordList.contains(allNodes.get(i)))
    		{
    			word = new String [2]; 
    			word[0] = allNodes.get(i); 
    			word[1] = allNodes.get(i-1).split(" ")[0]; 
    			allWords.add(word); 
    		}
    	}

    	for(int i = 0; i < dependencyList.size(); i++)
		{	
			TypedDependency curDep = dependencyList.get(i);
    		String depIndex = ""; 
    		depIndex += curDep.dep().index(); 
    		String govIndex = ""; 
    		govIndex += curDep.gov().index();
    		if(curDep.reln().toString().startsWith("conj")) // put all and/or information in conjTable; 
    		{
    			String conj = ""; 
    			conj += govIndex; 
    			conj += " "; 
    			conj += depIndex; 
    			query.conjTable.add(conj); 
    		}
        	String [] treeTableEntry = {depIndex, curDep.dep().value(), allWords.get(curDep.dep().index())[1], govIndex, curDep.reln().toString()}; 
			query.treeTable.add(treeTableEntry); // treeTableEntry is in format: depIndex, depValue, pos, govIndex, relationship; 
			// System.out.println(String.format("%s, %s, %s, %s, %s", depIndex, curDep.dep().value(), allWords.get(curDep.dep().index())[1], govIndex, curDep.reln().toString()));
		}


		  // DEBUG PRINTS
		if (CommandInterface.DEBUG_PRINTS == true) {
			System.out.println("\n============\nSTANFORD NLP PARSE TREE\n===============\n");
			TreePrint tp = new TreePrint("penn,typedDependenciesCollapsed");
			tp.printTree(parse);
		}

		// TreePrint tp = new TreePrint("penn,typedDependenciesCollapsed");
        // tp.printTree(parse);
	}
	
	public static void buildTree(Query query)
	{
		query.parseTree = new ParseTree(); 
		
        boolean[] doneList = new boolean[query.treeTable.size()]; // each tag should be visited once
        for(int i = 0; i < doneList.length; i++)
        {
        	doneList[i] = false; 
        }
        
    	for(int i = 0; i < query.treeTable.size(); i++) // create the root; 
    	{
    		if(query.treeTable.get(i)[3].equals("0"))
    		{
    			query.parseTree.buildNode(query.treeTable.get(i));
    			doneList[i] = true; 
    		}
    	}
    	
    	boolean finished = false;
    	while(finished == false)
    	{
    		for(int i = 0; i < query.treeTable.size(); i++)
    		{
    			if(doneList[i] == false)
    			{
    				if(query.parseTree.buildNode(query.treeTable.get(i)) == true)
    				{
    					doneList[i] = true; 
    					break; 
    				}
    			}
    		}
    		
        	finished = true;
        	for(int i = 0; i < doneList.length; i++)
        	{
        		if(doneList[i] == false)
        		{
        			finished = false; 
        			break; 
        		}
        	}
    	}
	}
	
	public static void fixConj(Query query)
	{
		if(query.conjTable.size() == 0)
		{
			return; 
		}
		
    	for(int i = 0; i < query.conjTable.size(); i++)
    	{
    		String conj = query.conjTable.get(i); 
    		int govNum = Integer.parseInt(conj.split(" ")[0]); 
    		int depNum = Integer.parseInt(conj.split(" ")[1]); 
    		ParseTreeNode govNode = query.parseTree.searchNodeByOrder(govNum);
    		ParseTreeNode depNode = query.parseTree.searchNodeByOrder(depNum); 

    		String logic = ","; 
    		if(query.parseTree.searchNodeByOrder(depNode.wordOrder-1) != null)
    		{
        		logic = query.parseTree.searchNodeByOrder(depNode.wordOrder-1).label; 
    		}
    		if(logic.equalsIgnoreCase("or"))
    		{
    			query.conjTable.set(i, query.conjTable.get(i)); 
				depNode.leftRel = "or"; 
				for(int j = 0; j < govNode.parent.children.size(); j++)
				{
					if(govNode.parent.children.get(j).leftRel.equals(","))
					{
						govNode.parent.children.get(j).leftRel = "or"; 
					}
				}
    		}
    		else if(logic.equalsIgnoreCase("and") || logic.equalsIgnoreCase("but"))
    		{
    			query.conjTable.set(i, query.conjTable.get(i)); 
				depNode.leftRel = "and"; 
				
				for(int j = 0; j < govNode.parent.children.size(); j++)
				{
					if(govNode.parent.children.get(j).leftRel.equals(","))
					{
						govNode.parent.children.get(j).leftRel = "and"; 
					}
				}
    		}
    		else if(logic.equalsIgnoreCase(","))
    		{
				depNode.leftRel = ","; 
    		}

    		depNode.parent = govNode.parent; 
    		govNode.parent.children.add(depNode); 
    		govNode.children.remove(depNode); 
    		depNode.relationship = govNode.relationship; 
    	}
	}


	/**
     * Return statistics      
     */
    public static Table getStatistics(ParseTree parseTree) {
        List<String> columnTitles = new ArrayList<>();   // The column titles.
        List<Table.Row> rows = new ArrayList<>();        // The table rows.

        // Check if parse tree was not created.
        if (parseTree != null)  { 
            // Each row will contain 2 values , the keyword and it's pos tag.
            columnTitles.addAll(Arrays.asList("Keyword", "Tag"));  

            // Traverse the tree
            Queue<ParseTreeNode> queue = new LinkedList<>();  // For the BFT            

            // Initialize the queue and traverse the tree
            queue.add(parseTree.root);
            while (!queue.isEmpty()) {
                ParseTreeNode node = queue.poll();

                // Add the node's children in the queue.
                for (ParseTreeNode child: node.children)
                    queue.add(child);

                // Get the statistics from each node.                
				if (node != parseTree.root)
					if (PosTags.map.get(node.pos) != null)
						rows.add(new Table.Row(
							Arrays.asList(
								node.label , PosTags.map.get(node.pos)
						)));
            }                     
        }

        // Return the table.
        return new Table("Pos Tags", columnTitles, rows);
    }
	
}
