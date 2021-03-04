package shared.database.model;

import shared.database.connectivity.DatabaseIndexCreator;

public class PostgreSQLQueries extends SQLQueries {


    public final static String INV_INDEX_COND = "%s @@ to_tsquery(%s)";  




    /**************************************************
     * Queries fetching information from the database *
     ***************************************************/

    // Sets the search path for the connection.
    public static final String SET_SEARCH_PATH = "SET search_path TO %s, public";

    // Fetch all the column in the database.
    // source : https://dataedo.com/kb/query/postgresql/list-table-columns-in-database
    public static final String INFORMATION_SCHEMA_COLUMNS_QUERY =
        "SELECT COLUMN_NAME, TABLE_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, TABLE_SCHEMA " +
        "FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE table_schema not in ('information_schema', 'pg_catalog')"; 


    // Gets the primary keys of all tables
    public static final String TABLES_PRIMARY_KEY_COLUMNS_QUERY =
      "SELECT tc.table_name, kc.column_name " +
      "FROM information_schema.table_constraints tc " +
        "JOIN information_schema.key_column_usage kc " + 
          "ON kc.table_name = tc.table_name and kc.table_schema = tc.table_schema and kc.constraint_name = tc.constraint_name " +
      "WHERE tc.constraint_type = 'PRIMARY KEY' " +
        "AND kc.ordinal_position is not null";


    // Gets the foreign keys of all tables
    // First two columns are the {Table, Foreign key} pair and the next two {ReferencedTable, ReferencedColumn}
    public static final String TABLES_FOREIGN_KEY_COLUMNS_QUERY =
      "SELECT tc.table_name, kcu.column_name, ccu.table_name as REFERENCED_TABLE_NAME, ccu.column_name as REFERENCED_COLUMN_NAME " +
      "FROM information_schema.table_constraints tc, information_schema.constraint_column_usage ccu, information_schema.key_column_usage kcu  " +
      "WHERE tc.constraint_type='FOREIGN KEY' " +
          "AND tc.constraint_catalog=ccu.constraint_catalog  " +
          "AND tc.constraint_schema=ccu.constraint_schema  " +
          "AND tc.constraint_name=ccu.constraint_name " +
          "AND tc.constraint_catalog=kcu.constraint_catalog " +
          "AND tc.constraint_schema=kcu.constraint_schema " +
          "AND tc.constraint_name=kcu.constraint_name";


    public static final String GET_GIN_GIX_INDEXES =
      "SELECT tablename, indexname " + 
      "FROM pg_indexes " + 
      "WHERE indexdef LIKE '% gin %' OR indexdef like '% gist %'";


    public static final String TABLE_ROW_COUNT_QUERY =
      "SELECT relname as TABLE_NAME, n_live_tup as TABLE_ROWS " +
      "FROM pg_stat_user_tables";

    public static final String UPDATE_TABLE_SET_TSVECTOR = 
      "UPDATE %s SET %s = to_tsvector('english', coalesce(%s,''))";

    public static final String CREATE_GIN_INDEX = 
      "CREATE INDEX %s ON %s USING GIN (%s)";
      
    // TRANSACTION COMMANDS
    public static final String BEGIN_TRANSACTION = "BEGIN TRANSACTION";
    public static final String COMMIT_CHANGES = "COMMIT";
    public static final String ROLLBACK = "ROLLBACK";
}