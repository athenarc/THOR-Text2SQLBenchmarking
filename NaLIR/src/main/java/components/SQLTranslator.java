package components;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import architecture.CommandInterface;
import rdbms.RDBMS;
import rdbms.SchemaElement;
import dataStructure.Block;
import dataStructure.ParseTree;
import dataStructure.ParseTreeNode;
import dataStructure.Query;
import dataStructure.SystemResult;

public class SQLTranslator
{
	public static List<SystemResult> translate(Query query, RDBMS db)
	{
		List<SystemResult> results = new ArrayList<>();
		preStructureAdjust(query);

		// Debug Print
		if (CommandInterface.DEBUG_PRINTS == true) {
			System.out.println("\n------------\n% TRANSLATOR %\n------------\n");
			System.out.println("\n-----\nTREE\n------\n" + query.queryTree.toString());
		}
		// ----

		if(query.queryTree.allNodes.size() < 2)
		{
			return results;
		}

		query.blocks = new ArrayList<Block>();
		blockSplit(query);
		query.blocks.get(0).nodeEdgeGen(query.mainBlock, query.queryTree, query.graph);
		query.blocks.get(0).translate(query.mainBlock, query.queryTree);
		query.translatedSQL = query.blocks.get(0).SQL;

		// SQL Print
		// System.out.println("QUERY TO EXECUTE :\n" + query.translatedSQL + "\n");

		Set<String> networks = extractNetworks(query.mainBlock);
		query.finalResult = db.conductSQL(query.translatedSQL, results, networks);

		return results;
	}


	public static Set<String> extractNetworks(Block block) {
		Set<String> tables = new HashSet<>();
		if (block == null) return tables;

		// Call the same function for all other inner blocks
		for (Block innerBlock: block.innerBlocks) {
			tables.addAll( extractNetworks(innerBlock));
		}

		// Fill the set from blocks from elements
		if (block.fromElements != null) {
			for(int i = 0; i < block.fromElements.size(); i++) {
				if (block.fromElements.get(i) instanceof SchemaElement){
					tables.add( ((SchemaElement) block.fromElements.get(i)).name );
				}
			}
		}

		// Return the set of blocks
		return tables;
	}

	public static void preStructureAdjust(Query query)
	{
		if(query.queryTree.allNodes.get(0) != null && query.queryTree.allNodes.get(0).children.size() > 1)
		{
			for(int i = 1; i < query.queryTree.allNodes.get(0).children.size(); i++)
			{
				ParseTreeNode OT = query.queryTree.allNodes.get(0).children.get(i);
				if(OT.children.size() == 2)
				{
					ParseTreeNode left = OT.children.get(0);
					ParseTreeNode right = OT.children.get(1);
					if(right.function.equals("max") || right.function.equals("min"))
					{
						if(right.children.size() == 0)
						{
							components.NodeInserter.addASubTree(query.queryTree, right, left);
						}
					}
				}
			}
		}
	}

	public static void blockSplit(Query query)
	{
		ParseTree queryTree = query.queryTree;

		ArrayList<ParseTreeNode> nodeList = new ArrayList<ParseTreeNode>();
		nodeList.add(queryTree.allNodes.get(0));

		while(!nodeList.isEmpty())
		{
			ParseTreeNode curNode = nodeList.remove(nodeList.size()-1);
			Block newBlock = null;
			if(curNode.parent != null && curNode.parent.tokenType.equals("CMT"))
			{
				newBlock = new Block(query.blocks.size(), curNode);
				query.blocks.add(newBlock);
			}
			else if(curNode.tokenType.equals("FT") && !curNode.function.equals("max"))
			{
				newBlock = new Block(query.blocks.size(), curNode);
				query.blocks.add(newBlock);
			}

			for(int i = curNode.children.size()-1; i >= 0; i--)
			{
				nodeList.add(curNode.children.get(i));
			}
		}

		ArrayList<Block> blocks = query.blocks;
		if(blocks.size() == 0)
		{
			return;
		}

		Block mainBlock = blocks.get(0);
		for(int i = 0; i < blocks.size(); i++)
		{
			ParseTreeNode curRoot = blocks.get(i).blockRoot;
			while(curRoot.parent != null)
			{
				if(curRoot.parent.tokenType.equals("CMT"))
				{
					mainBlock = blocks.get(i);
					break;
				}
				curRoot = curRoot.parent;
			}
		}
		query.mainBlock = mainBlock;

		for(int i = 0; i < blocks.size(); i++)
		{
			Block block = blocks.get(i);
			if(block.blockRoot.parent.tokenType.equals("OT"))
			{
				block.outerBlock = mainBlock;
				mainBlock.innerBlocks.add(block);
			}
			else if(block.blockRoot.parent.tokenType.equals("FT"))
			{
				for(int j = 0; j < blocks.size(); j++)
				{
					if(blocks.get(j).blockRoot.equals(block.blockRoot.parent))
					{
						block.outerBlock = blocks.get(j);
						blocks.get(j).innerBlocks.add(block);
					}
				}
			}
		}
	}
}