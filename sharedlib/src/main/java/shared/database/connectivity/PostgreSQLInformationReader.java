package shared.database.connectivity;

import shared.database.model.SQLDatabase;
import shared.database.model.SQLTable;
import shared.database.model.SQLType;
import shared.util.PrintingUtils;
import shared.database.model.PostgreSQLQueries;
import shared.database.model.SQLColumn;
import shared.database.model.SQLForeignKeyConstraint;
import shared.database.model.SQLQueries;

import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Reads a MySQL Database
 */
public class PostgreSQLInformationReader implements DatabaseInfoReader {


    /**
     * Uses the INFORMATION_SCHEMA to get the tables and columns of a database.
     * Saves the information in the requested database object.
     * 
     * @param database.
     */
    public static void getTableAndColumnNames(SQLDatabase database) {
        
        Map<String, SQLTable> tablesMap = new HashMap<String, SQLTable>();  // A hash map that maps table names to SQLTable objects.
        HashSet<String> schemas = new HashSet<>();                          // A hash set holding schema names, later we will set the path to those schemas.

        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // This query returns all the columns in the database.
            // From the information of the columns we will extract the table names, too.
            con = DataSourceFactory.getConnection();
            stmt = con.prepareStatement(PostgreSQLQueries.INFORMATION_SCHEMA_COLUMNS_QUERY);            
            rs = stmt.executeQuery();

            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String columnType = rs.getString("DATA_TYPE");
                Long columnsCharacterMaximumLength = rs.getLong("CHARACTER_MAXIMUM_LENGTH");

                // Skip the "avg_length" table since it is a temporary table that we use to save statistics.
                if (tableName.equals("avg_length")) { continue; }
                if (tableName.equals("size")) { continue; }
                if (tableName.equals("history")) { continue; }
                if (tableName.contains("soda")) { continue; }

                // Store the schema name.
                schemas.add(rs.getString("TABLE_SCHEMA"));

                // If an SQLTable with the same name exists, just add the column.
                if (tablesMap.containsKey(tableName)) {
                    // Create a column.
                    SQLType columnSqlType = new SQLType(columnType, columnsCharacterMaximumLength);
                    SQLColumn column = new SQLColumn(tablesMap.get(tableName), columnName, columnSqlType);

                    // Add it to the table.
                    tablesMap.get(tableName).addColumn(column);
                }
                else {
                    // Create a new SQLTable and then add the column.
                    SQLTable newTable = new SQLTable(tableName);

                    // Create column
                    SQLType columnSqlType = new SQLType(columnType, columnsCharacterMaximumLength);
                    SQLColumn column = new SQLColumn(newTable, columnName, columnSqlType);
                    newTable.addColumn(column);
                    tablesMap.put(tableName, newTable);
                }
            }

            // Execute a set search path stmt. This way the tables wont need a prefix of their schema.
            DatabaseUtil.close(stmt, rs);
            String schemasList = PrintingUtils.separateWithDelimiter(schemas, ",");            
            stmt = con.prepareStatement(String.format(PostgreSQLQueries.SET_SEARCH_PATH, schemasList));
            stmt.executeUpdate();
            
            // Then fetch primary keys all tables.
            DatabaseUtil.close(stmt);            
            stmt = con.prepareStatement(PostgreSQLQueries.TABLES_PRIMARY_KEY_COLUMNS_QUERY);            
            rs = stmt.executeQuery();
            while (rs.next()) {

                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");

                SQLTable table = tablesMap.get(tableName);
                if (table == null) {
                    System.out.println("[INF] Table: " + tableName + " was not found in our db object... skipping pk extraction");
                    continue;
                }
                table.getPrimaryKey().add(table.getColumnByName(columnName));       // Add pk to table 
                table.getColumnByName(columnName).addKey(SQLColumn.PK_IDENTIFIER);  // Add pk identifier to column

            }

            // Add all tables in the database.
            for (SQLTable table : tablesMap.values())
                database.addTable(table);

        } catch (SQLException e) {
			e.printStackTrace();
        }
        finally {
            // Close the connection
            DatabaseUtil.close(con, stmt, rs);
        }        
    }

     /**
     * Uses the INFORMATION_SCHEMA to get the foreign key constraints of a database.
     * Saves the information in the requested database objec
     * 
     * @param database.
     */    
    public static void getFKConstraints(SQLDatabase database) {        
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // This query returns all the foreign key constraints in the schema.
            con = DataSourceFactory.getConnection();
            stmt = con.prepareStatement(PostgreSQLQueries.TABLES_FOREIGN_KEY_COLUMNS_QUERY);            
            rs = stmt.executeQuery();

            // Loop through every constraint.
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                String columnName = rs.getString("COLUMN_NAME");
                String referencedTableName = rs.getString("REFERENCED_TABLE_NAME");
                String referencedColumnName = rs.getString("REFERENCED_COLUMN_NAME");

                // Create a foreign key constraint and add it to the
                // database's list of foreign key constraints.
                SQLForeignKeyConstraint constraint = new SQLForeignKeyConstraint();
                constraint.fill(database, tableName, columnName, referencedTableName, referencedColumnName);
                database.addFKConstraint(constraint);

                // Also Add the column and the constrain in the Tables involving in the constrain.
                database.getTableByName(tableName).getColumnByName(columnName).addKey(SQLColumn.FK_IDENTIFIER);
                database.getTableByName(tableName).addForeignKey(database.getTableByName(tableName).getColumnByName(columnName));
                database.getTableByName(tableName).addReferencingForeignKeyConstrain(constraint);
                database.getTableByName(referencedTableName).addReferencedForeignKeyConstrain(constraint);
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection
            DatabaseUtil.close(con, stmt, rs);
        }
    }

    /**
     * Uses the INFORMATION_SCHEMA to get the columns which are indexed with a 'GIN' or 'GIX' index.
     */ 
    public static void getIndexedColumns(SQLDatabase database) {
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // This query returns all the columns indexed with a FULLTEXT index.
            con = DataSourceFactory.getConnection();
            stmt = con.prepareStatement(PostgreSQLQueries.GET_GIN_GIX_INDEXES);
            rs = stmt.executeQuery();

            // Loop through every column.
            while (rs.next()) {
                String tableName = rs.getString("tablename");
                String indexName = rs.getString("indexname");

                // Extract the column name from index name. 
                // SEE: DatabaseIndexCreator, to check how we create index names.
                String colName = indexName.split("_")[ indexName.split("_").length -1 ]; // The last token is the column name.
                SQLColumn col = database.getTableByName(tableName).getColumnByName(colName);
                if (col != null)
                    col.setIsIndexed(true);
                                            
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection
            DatabaseUtil.close(con, stmt, rs);
        }
    }

    // Uses the INFORMATION_SCHEMA to get some statistics for the tables and the columns with a FULLTEXT index.
    public static void getTableAndColumnStatistics(SQLDatabase database) {        
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Connect to the database.
            con = DataSourceFactory.getConnection();

            // This query returns the number of rows of all tables in the schema.            
            stmt = con.prepareStatement(PostgreSQLQueries.TABLE_ROW_COUNT_QUERY);            
            rs = stmt.executeQuery();

            // Loop through every table and save its number of rows.
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                int tableRows = rs.getInt("TABLE_ROWS");

                // Skip the "avg_length" table since it is a temporary table that we use to save statistics.
                if (tableName.equals("avg_length")) { continue; }
                if (tableName.equals("size")) { continue; }
                if (tableName.equals("history")) { continue; }
                if (tableName.contains("soda")) { continue; }

                // Handle views also ? 
                if (database.getTableByName(tableName) != null)
                    database.getTableByName(tableName).setRowsNum(tableRows);
            }

            // This query returns the average length in words of all columns
            // with a FULLTEXT index in the current database schema.
            try {
                stmt = con.prepareStatement(SQLQueries.COLUMN_AVERAGE_LENGTH_QUERY);
                rs = stmt.executeQuery();
    
                // Loop through every column and get its average length.
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    double averageLength = rs.getDouble("AVG_LENGTH");
                    database.getTableByName(tableName).getColumnByName(columnName).setAverageLength(averageLength);
                }   
            } catch (SQLException e) {
                System.out.println("[INFO] Could not find an avg_length table in the database");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection
            DatabaseUtil.close(con, stmt, rs);
        }
    } 
}