package discover.components.executors;

import shared.database.connectivity.DatabaseUtil;
import shared.database.model.graph.SchemaGraph;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLQuery;
import shared.database.connectivity.DataSourceFactory;
import discover.model.JoiningNetworkOfTupleSets;
import discover.model.OverloadedTuple;
import discover.DiscoverApplication;
import discover.components.SQLQueryCreator;
import discover.exceptions.JoinCandidateNotFoundException;
import discover.model.execution.CandidateNetworkAssignment;
import discover.model.execution.JoinableFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mysql.jdbc.exceptions.MySQLTimeoutException;

// Input : An Candidate Network Creation Assignment.
// Output : The tuples contained in the Candidate Network 
//          printed in the standard output.
// The CandidateNetworkAssignmentExecutor executes a Select query
// in the database and prints the tuples contained in the 
// Candidate Network that the input assignment creates. The 
// assignment may contain a join of Intermediate Results and Tuple Sets, 
// or a single Tuple Set. In the First case the query joins the Tables
// (or Temp Tables) and returns the wanted tuples. In the Last case the 
// tuples contained in the Tuple Sets are the Executors result.
public class CandidateNetworkAssignmentExecutor {

    private static int timeout = 30;
   
    // Executes the assignment. All the results are returned to the user as output.    
    public static List<OverloadedTuple> execute(CandidateNetworkAssignment assignment, SchemaGraph schemaGraph,
            SQLDatabase database) throws JoinCandidateNotFoundException {            
        List<OverloadedTuple> results = new ArrayList<OverloadedTuple>(); // The results to be returned.
        
        // Create a generic joinable format from the assignments Terms.
        JoinableFormat joinableFormat = new JoinableFormat();
        if (assignment.getAssignmentTerms() instanceof JoiningNetworkOfTupleSets)
            joinableFormat.fill( (JoiningNetworkOfTupleSets) assignment.getAssignmentTerms(), schemaGraph);
        else 
            joinableFormat.fill(assignment.getAssignmentTerms(), schemaGraph);
        
        // Create a SELECT query.
        SQLQuery selectQuery = SQLQueryCreator.createSQLSelectQuery(joinableFormat);
        

        // Create the network
        Set<String> networks = assignment.getAssignmentTerms().getContainedBaseTables();

        // Debug prints
        if (DiscoverApplication.DEBUG_PRINTS)
            System.out.println("Select Query :" + selectQuery.toSelectQuery() + "\n");        

        
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        // Execute the SELECT query against the MYSQL server.
        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();
            stmt = con.createStatement();

            // Execute the query.
            stmt.setQueryTimeout(timeout);
            rs = stmt.executeQuery(selectQuery.toSelectQuery());
            while (rs.next()) {                
                // Add the tuples to the List.
                OverloadedTuple tuple = new OverloadedTuple();
                tuple.fill(joinableFormat.getColumnsWithoutAliases(), rs);
                tuple.setQuery(selectQuery); tuple.setNetworks(networks);
                results.add(tuple);
            }
        }
        catch (MySQLTimeoutException e) {
            System.out.println("[INFO] An sql query was canceled due to timeout (thresh: " + timeout + ")");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt);
        }
        // Print the results.
        // System.out.println("Results for Assignment " + assignment.toAbbreviation() + "\n");
        // OverloadedTupleList tupleList = new OverloadedTupleList(results);
        // tupleList.print();

        return results;
    }

}