package shared.database.connectivity;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import shared.util.PrintingUtils;

import java.util.Date;

public class DatabaseUtil {

    private DatabaseUtil() {}
    
    public static boolean testConnection() {
        Connection con = null;
        Statement stmt = null;
        ResultSet   rs = null;

        // Test the db with a simple query
        try {
			con = DataSourceFactory.getConnection();
			stmt = con.createStatement();
			rs = stmt.executeQuery("select 1");
			if (rs != null) {
                System.out.println("#############Database Queried Successfully!################");
                return true;
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection
            close(con,stmt,rs);
        }

        return false;
    }

    public static Date toSqlDate(java.util.Date date) {
        return (date != null) ? new Date(date.getTime()) : null;
    }    
    
    /**
     * Prepare a phrase for a boolean search. In this function add the char 
     * '+' in front of every word in the parameter phrase. This way every 
     * world myst be preset in the results returned by the search.
     * 
     * @param phrase
     * @return
     */
    public static String prepareForAndBooleanSearch(String phrase) {        
        String booleanPhrase = "+";        
        int index = PrintingUtils.findNextChar(phrase, 0);

        while(index < phrase.length()) {
            // Get the char of the index position.
            char c = phrase.charAt(index);
                        
            // Add a + sign in case of white space
            if (PrintingUtils.isWhiteSpace(c)) {
                index = PrintingUtils.findNextChar(phrase, index);
                if (index <  phrase.length())
                    booleanPhrase += " +";                
            }
            else {
                // Add the char to the boolean phrase
                booleanPhrase += c;
                index++;
            }
        }

        return booleanPhrase;  
    }


    /**
     * Returns a phrase to use on a tsquery search that enforces that all terms 
     * in the phrase must be present to return a result.
     * 
     * @param phrase
     * @return
     */
    public static String prepareForAndTsQuerySearch(String phrase) {        
        String tsQueryPhrase = "";        
        int index = PrintingUtils.findNextChar(phrase, 0);

        while(index < phrase.length()) {
            // Get the char of the index position.
            char c = phrase.charAt(index);
                        
            // Add a & sign in case of white space
            if (PrintingUtils.isWhiteSpace(c)) {
                index = PrintingUtils.findNextChar(phrase, index);
                if (index <  phrase.length())
                    tsQueryPhrase += " & ";
            }
            else {
                // Add the char to the boolean phrase
                tsQueryPhrase += c;
                index++;
            }
        }

        return tsQueryPhrase;  
    }

    public static String escapeStrValue(String sqlValue) {
        String escapedStr = new String();

        // Escape char \
        escapedStr = PrintingUtils.escapeCharacter(sqlValue, '\\', '\\');

        // Escape char '
        escapedStr = PrintingUtils.escapeCharacter(sqlValue, '\'', '\\');
        
        // Escape char "
        escapedStr = PrintingUtils.escapeCharacter(escapedStr, '\"', '\\');

        return escapedStr;
    }

    public static void close(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                connection = null;
            }
            catch (SQLException e) {
                System.err.println("Closing Connection failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void close(Statement statement) {
        if (statement != null) {
            try {
                statement.close();
                statement = null;
            }
            catch (SQLException e) {
                System.err.println("Closing Statement failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
                resultSet = null;
            }
            catch (SQLException e) {
                System.err.println("Closing ResultSet failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public static void close(Connection connection, Statement statement) {
        close(statement);
        close(connection);
    }

    public static void close(Connection connection, Statement statement, ResultSet resultSet) {
        close(resultSet);
        close(statement);
        close(connection);
    }

    public static void close(Statement statement, ResultSet resultSet) {
        close(resultSet);
        close(statement);
    }

}
