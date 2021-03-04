package discoverIR.components.execution.executors;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import discoverIR.DiscoverIRApplication;
import discoverIR.components.SQLQueryCreator;
import discoverIR.components.execution.executors.Executor;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;

import discoverIR.exceptions.JoinCandidateNotFoundException;
import discoverIR.model.TupleSet;
import discoverIR.model.JoinableFormat;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.OverloadedTupleList;
import shared.database.model.SQLQuery;

// Input: A Candidate Network
// Output: The list of tuples contained in the Candidate Network.
public class CandidateNetworkExecutor extends Executor {

    private static Integer SQL_TIMEOUT = 30;

    public CandidateNetworkExecutor(SchemaGraph schemaGraph, SQLDatabase database,
            List<TupleSet> nonFreeTupleSets, Integer maxTuples) {
        super(database, schemaGraph, maxTuples, nonFreeTupleSets);
    }
    
    public OverloadedTupleList execute(JoiningNetworkOfTupleSets candidateNetwork) {
        // A Tuple List containing the results.
        List<OverloadedTuple> resultTuples = new ArrayList<OverloadedTuple>();
        
        // In case of a null Candidate Network return no results.
        if (candidateNetwork == null) {
            System.err.println("[ERR] CandidateNetworkExecutor.execute : Null Candidate Network.");            
            return new OverloadedTupleList(resultTuples); // Return an empty tuple list.
        }                    

        // Create a joinableFormat out of the candidateNetwork.
        JoinableFormat joinableFormat = new JoinableFormat();
        try {
            joinableFormat.fill(candidateNetwork, super.schemaGraph);
        } catch (JoinCandidateNotFoundException e1) {
            e1.printStackTrace();
            return new OverloadedTupleList(resultTuples); // Return an empty tuple list.
		}

        // The SQL SELECT query.
        SQLQuery query = SQLQueryCreator.createSQLSelectQuery(joinableFormat);
        String selectQuery = query.toSelectQuery();


        if (DiscoverIRApplication.DEBUG_PRINTS) {
            System.out.println("RESULT\nJNTS : " + candidateNetwork.toAbbreviation());
            System.out.println("Query :" + selectQuery + "\n");
            // System.out.println("Pretty Select Query :" + query.toPrettyQuery() + "\n");
        }

        // Initialize connection variables.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();

            // Create and execute the statement.
            stmt = con.createStatement();
            stmt.setQueryTimeout(SQL_TIMEOUT);           
            rs = stmt.executeQuery(selectQuery);

            // Get the results
            while(rs.next()) {
                OverloadedTuple tuple = new OverloadedTuple();
                tuple.fill(
                    joinableFormat.getColumnsWithoutAliases(), rs, candidateNetwork.getSize()
                );
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
            OverloadedTupleList otl = new OverloadedTupleList(resultTuples);
            otl.setCnGeneratedThis(candidateNetwork.toAbbreviation());
            return otl;
        }
    }    
   
}
