package rdbms;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import dataStructure.ParseTreeNode;
import dataStructure.SystemResult;

public class RDBMS 
{
	public SchemaGraph schemaGraph; 
	public Connection conn;

	static final Integer  SQL_TIMEOUT = 30;

	public ArrayList<String> history = new ArrayList<String>(); 

	public RDBMS(String database_name) throws Exception
	{
		String driver = "com.mysql.jdbc.Driver"; 
		String db_url = "jdbc:mysql://127.0.0.1:3306/?useSSL=false";
		String user = "root";
		String password = "toor";
		Class.forName(driver);
		conn = DriverManager.getConnection(db_url, user, password);
		
		Statement statement = conn.createStatement(); 
		statement.execute("use " + database_name); 
		loadHistory(database_name); 

		schemaGraph = new SchemaGraph(database_name.toLowerCase()); // TODO files use "mas" but DB name is MAS
	}
	
	public ArrayList<ArrayList<String>> conductSQL(String query, List<SystemResult> results, Set<String> networks)
	{
		ArrayList<ArrayList<String>> finalResults = new ArrayList<ArrayList<String>>(); 
		try
		{
			Statement statement = conn.createStatement(); 
			statement.setQueryTimeout(SQL_TIMEOUT);
			ResultSet result = statement.executeQuery(query);
			while(result.next())
			{
				results.add(new SystemResult(result, query, networks));
				int columnSize = result.getMetaData().getColumnCount(); 
				ArrayList<String> row = new ArrayList<String>(); 
				for(int i = 0; i < columnSize; i++)
				{
					row.add(result.getString(i+1)); 
				}
				finalResults.add(row); 
			}
			
			return finalResults; 
		} catch(Exception e)
		{
			return new ArrayList<ArrayList<String>>(); 
		}
	}

	
	public void loadHistory(String database) throws SQLException
	{
		Statement statement = conn.createStatement(); 
		String query = "SELECT * FROM " + database + ".history; "; 
		ResultSet results = statement.executeQuery(query); 
		
		while(results.next())
		{
			history.add(results.getString(2)); 
		}
	}
	
	public boolean isSchemaExist(ParseTreeNode treeNode) throws Exception 
	{
		ArrayList<SchemaElement> attributes = schemaGraph.getElementsByType("text number"); 		

		for(int i = 0; i < attributes.size(); i++)
		{
			MappedSchemaElement element = attributes.get(i).isSchemaExist(treeNode.label); 
			if(element != null)
			{
				treeNode.mappedElements.add(element); 
			}
		}
		if(!treeNode.mappedElements.isEmpty())
		{
			return true; 
		}
		else
		{
			return false;
		}
	}
	
	public boolean isTextExist(ParseTreeNode treeNode) throws Exception 
	{
		ArrayList<SchemaElement> textAtts = schemaGraph.getElementsByType("text"); 		
		for(int i = 0; i < textAtts.size(); i++)
		{

			MappedSchemaElement textAtt = null;
			try {
				textAtt = textAtts.get(i).isTextExist(treeNode.label, conn); 	
			} catch (Exception e) {
				// e.printStackTrace();
			}
			finally {
				if(textAtt != null)
				{			
					treeNode.mappedElements.add(textAtt); 
				}
			}
		}
		
		if(!treeNode.mappedElements.isEmpty())
		{
			return true; 
		}
		return false;
	}

	public boolean isNumExist(String operator, ParseTreeNode treeNode) throws Exception 
	{
		ArrayList<SchemaElement> textAtts = schemaGraph.getElementsByType("number"); 
		for(int i = 0; i < textAtts.size(); i++)
		{
			MappedSchemaElement textAtt = textAtts.get(i).isNumExist(treeNode.label, operator, conn); 
			if(textAtt != null)
			{
				treeNode.mappedElements.add(textAtt); 
			}
		}
		
		if(!treeNode.mappedElements.isEmpty())
		{
			return true; 
		}
		return false;
	}
}
