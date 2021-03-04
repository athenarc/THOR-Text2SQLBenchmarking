package expressq2.components;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.exceptions.MySQLTimeoutException;

import expressq2.model.OverloadedTuple;
import expressq2.model.OverloadedTupleList;
import expressq2.model.SQLQuery;

import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLDatabase;

/**
 * This class models an Query Executor for an underling SQL Database. Turns the
 * SQL Query into a list of tuples returned by the execution of the query.
 */
public class SQLQueryExecutor {

    private static int SQL_TIMEOUT = 60;
    
    public static OverloadedTupleList executeQuery(SQLQuery sqlQuery, SQLDatabase database, Double interpretationScore) {
        // A Tuple List containing the results.
        List<OverloadedTuple> resultTuples = new ArrayList<OverloadedTuple>();        

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
            rs = stmt.executeQuery(sqlQuery.getQueryToString());

            // Get the results
            while(rs.next()) {
                OverloadedTuple tuple = new OverloadedTuple();
                tuple.fill(database, rs );
                tuple.setQuery(sqlQuery);
                tuple.setScore(interpretationScore);
                resultTuples.add(tuple);
            }

        }
        catch (MySQLTimeoutException e) {
            System.err.println("Query reached our time out limit (" + SQL_TIMEOUT + ")");
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt, rs);
        }

        // Return the Results        
        if (resultTuples.isEmpty())            
            return new OverloadedTupleList(new ArrayList<OverloadedTuple>());
        else            
            return new OverloadedTupleList(resultTuples);     
    }
}  