package shared.database.connectivity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

import shared.database.config.PropertiesSingleton;
import shared.database.model.DatabaseType;
import shared.database.model.MySqlDatabase;
import shared.database.model.PostgreSQLDatabase;
import shared.database.model.PostgreSQLQueries;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLQueries;

/**
 * Create full-text indexes in the underlying database.
 */
public class DatabaseIndexCreator {

    public final static String INDEXED_COLUMN_PREFIX = "ts_vec_";
    private final static String VEC_NAME = INDEXED_COLUMN_PREFIX + "%s";
    private final static String IDX_NAME = "ts_idx_%s_%s";


    /**
     * Check if the column matches any of the other columns names in the string list.
     * Columns can have 2 formats:
     *  - "tableName.ColumnName"
     *  - "ColumnName"
     *
     * @param col
     * @param columnsToExclude
     * @return
     */
    private static boolean columnMatchesStringList(SQLColumn col, List<String> columnsToExclude) {
        //  Loop all columns
        for (String colToEx: columnsToExclude) {
            // If column to exclude contains table table name then check column with the table name.
            // else simply check if the names are equal
            if (colToEx.contains(".")) {
                if (colToEx.equals( col.getTableName() + "." + col.getName())) return true;
            } else {
                if (colToEx.equals(col.getName())) return true;
            }
        }
        return false;
    }


    /**
     * Create full-text indexes for all textual attributes in a psql database excluding some columns.
     * How indexes are crated:
     * - Add a new column to the table, for each textual attribute.
     * - This column contains the column values tokenized & stemmed ( to_tsvector() ), named like: ts_vec_<column>
     * - Create a GIN index named with the format: ts_idx_<table>_<column>
     *
     * @param columnsToExclude
     * @param database
     */
    public static void createIndex(List<String> columnsToInclude, PostgreSQLDatabase database) {

        // Initialize connection variables
        Connection con = null;
        Statement stmt = null;
        try {
            // Get the connection
            con = DataSourceFactory.getConnection();
            // con.setAutoCommit(false);

            // Loop all column in the database
            for (SQLColumn col: database.getAllColumns()) {
                // Skip not matches columns and non textual columns and indexed columns
                if ( !columnMatchesStringList(col, columnsToInclude)  || !col.getType().isTextual() || col.isIndexed())  continue;
                System.out.println("[INFO] Creating index for: " + col.toString());


                // Initialize names
                String newColName = String.format(VEC_NAME, col.getName());
                String idxName = String.format(IDX_NAME, col.getTableName(), col.getName());

                // Create Alter table & Update & Create Index queries
                String alterTableSQL = String.format(SQLQueries.SQL_ADD_COLUMN_QUERY,
                    col.getTableName(), newColName, "tsvector");
                String updateTableSQL = String.format(PostgreSQLQueries.UPDATE_TABLE_SET_TSVECTOR,
                    col.getTableName(), newColName, col.getName());
                String createIndexSQL = String.format(PostgreSQLQueries.CREATE_GIN_INDEX,
                    idxName, col.getTableName(), newColName);

                // Issue the queries.
                stmt = con.createStatement();
                stmt.executeUpdate(alterTableSQL);
                DatabaseUtil.close(stmt);

                stmt = con.createStatement();
                stmt.executeUpdate(updateTableSQL);
                DatabaseUtil.close(stmt);

                stmt = con.createStatement();
                stmt.executeUpdate(createIndexSQL);
                DatabaseUtil.close(stmt);
            }
            // con.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            // try {
            //     System.out.println("Rolling back...");
            //     con.rollback();
            // } catch(SQLException excep) {
            //     e.printStackTrace();
            // }
        } finally {
            // try {
            //     con.setAutoCommit(true);
            // } catch (SQLException e) {
            //     e.printStackTrace();
            // }
            DatabaseUtil.close(con, stmt);
        }
    }

        /**
     * Create full-text indexes for all textual attributes in a psql database excluding some columns.
     * How indexes are crated:
     * - Create a FULLTEXT index on every column requested by the paramter
     *
     * @param columnsToExclude
     * @param database
     */
    public static void createIndex(List<String> columnsToInclude, MySqlDatabase database) {

        // Initialize connection variables
        Connection con = null;
        Statement stmt = null;
        try {
            // Get the connection
            con = DataSourceFactory.getConnection();

            // Loop all column in the database
            for (SQLColumn col: database.getAllColumns()) {
                // Skip not matches columns and non textual columns.
                if ( !columnMatchesStringList(col, columnsToInclude)  || !col.getType().isTextual() || col.isIndexed())  continue;
                System.out.println("[INFO] Creating index for: " + col.toString());


                // Initialize names
                String idxName = String.format(IDX_NAME, col.getTableName(), col.getName());

                // Create Alter table & Update & Create Index queries
                String alterTableSQL = String.format(SQLQueries.MYSQL_CREATE_INV_INDEX, idxName, col.getTableName(), col.getName());

                // Issue the queries.
                try {
                    stmt = con.createStatement();
                    stmt.executeUpdate(alterTableSQL);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            DatabaseUtil.close(con, stmt);
        }
    }


    public static void main(String[] args) {
        PropertiesSingleton.loadPropertiesFile("app");
        SQLDatabase database = SQLDatabase.InstantiateDatabase("cordis", DatabaseType.MySQL);
        System.out.println(database);
        List<String> columnsToInclude = new ArrayList<>( Arrays.asList("topics.title", "projects.acronym", "projects.title", "subject_areas.title", "erc_panels.description", "programmes.title", "project_members.member_name",
            "aproject_members.ctivity_type", "street", "city", "member_role", "member_short_name", "name", "eu_territorial_units.description" ) );
        DatabaseIndexCreator.createIndex(columnsToInclude, (MySqlDatabase) database);
    }

}