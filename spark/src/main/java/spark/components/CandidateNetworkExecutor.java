package spark.components;

import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;

import spark.exceptions.JoinCandidateNotFoundException;
import spark.model.JoiningNetworkOfTupleSets;
import spark.model.OverloadedTuple;
import spark.model.OverloadedTupleList;
import spark.model.JoinableFormat;
import spark.model.TupleSet;
import spark.components.SQLQueryCreator;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import shared.database.model.SQLQuery;

// Input: A candidate network.
// Output: The list of joining (trees of) tuples of the network.
public class CandidateNetworkExecutor {

    protected SQLDatabase database; // The database instance.    
    protected SchemaGraph schemaGraph; // The schema graph of the database.
    protected List<TupleSet> nonFreeTupleSets; // A list of all non-free tuple sets used in the execution plan.    
    protected Integer maxTuples; // The maximum number of results to return.    

    static final Integer SQL_TIMEOUT = 30;

    public CandidateNetworkExecutor(SchemaGraph schemaGraph, SQLDatabase database, List<TupleSet> nonFreeTupleSets, Integer maxTuples) {
        this.database = database;        
        this.maxTuples = maxTuples;
        this.schemaGraph = schemaGraph; // Copy the schema graph.        
    }

    public OverloadedTupleList execute(JoiningNetworkOfTupleSets candidateNetwork) {
        List<OverloadedTuple> resultTuples = new ArrayList<OverloadedTuple>(); // A list to save the results.
        
        // In case of a null candidate network return no results.
        if (candidateNetwork == null) {
            System.err.println("[ERR] CandidateNetworkExecutor.execute: Null Candidate Network.");            
            return new OverloadedTupleList(resultTuples); // Return an empty tuple list.
        }

        // Create a joinableFormat object out of the candidate network.
        JoinableFormat joinableFormat = new JoinableFormat();
        try {
            joinableFormat.fill(candidateNetwork, this.schemaGraph);
        } catch (JoinCandidateNotFoundException e1) {
            e1.printStackTrace();
            return new OverloadedTupleList(resultTuples); // Return an empty tuple list.
		}

        // Create the SQL SELECT query to execute.
        SQLQuery query = SQLQueryCreator.createSQLSelectQuery(joinableFormat);
        String selectQuery = query.toSelectQuery();

        // System.out.println("Query: " + selectQuery + "\n");

        // Initialize the connection variables.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = DataSourceFactory.getConnection();

            // Create and execute the statement.
            stmt = con.createStatement();
            stmt.setQueryTimeout(SQL_TIMEOUT);
            rs = stmt.executeQuery(selectQuery);

            // Get the results.
            while(rs.next()) {
                OverloadedTuple tuple = new OverloadedTuple();
                tuple.fill(joinableFormat.getColumnsWithoutAliases(), rs);
                tuple.setQuery(query);
                resultTuples.add(tuple);
            }

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt, rs);
        }

        // Return the results.
        if (resultTuples.isEmpty()) {
            return new OverloadedTupleList(new ArrayList<OverloadedTuple>());
        }
        else {
            return new OverloadedTupleList(resultTuples);
        }
    }
  
}

