package shared.database .model;


public class SQLQueries {
    // The Distinct specifier.
    public final static String DISTINCT_SPECIFIER = "DISTINCT";

    // Limit Statement for the SQL Query.
    public final static String LIMIT_STATEMENT = "LIMIT %d";
    public final static String LIMIT_STATEMENT_2K = " LIMIT 2000";

    // The alias specifier.
    public final static String ALIAS_SPECIFIER = " as ";

    // The keyword for adding more Statements in the WHERE part of a query.
    public static final String ADD_WHERE_STMT = " AND ";

    // A letter used to create an alias for a table.
    public static final String ALIAS_LETTER = "t";

    public static final String SQL_MATCH_AGAINST_STMT = "match(%s) against('%s' IN BOOLEAN MODE)";
    public static final String INV_INDEX_CONDITION = "match(%s) against(%s IN BOOLEAN MODE)";

    public static final String LIKE_STMT = "%s LIKE ?";

    // A Query that disables the FK constraints
    public static final String SQL_DISABLE_CONSTRAINTS_QUERY = "SET FOREIGN_KEY_CHECKS=0";

    // A Query that enables the FK constraints
    public static final String SQL_ENABLE_CONSTRAINTS_QUERY = "SET FOREIGN_KEY_CHECKS=1";

    // A Drop Table query for the TempTable.
    public static final String SQL_DROP_TABLES_QUERY = "DROP TABLE IF EXISTS %s";

    // A Drop View query
    public static final String SQL_DROP_VIEWS_QUERY = "DROP VIEW IF EXISTS %s";


    // Primary Key constraint for SQL Queries
    public final static String PRIMARY_KEY_CONSTRAINT = "PRIMARY KEY (%s)";

    // Foreign Key constraint for SQL Queries
    public final static String FOREIGN_KEY_CONSTRAINT = "FOREIGN KEY (%s) REFERENCES %s(%s)";

    // The In List Constraint.
    public final static String IN_LIST_CONSTRAINT = "%s IN (%s)";

    // Column name, Value pair for an SQL query.
    public final static String COLUMN_VALUE_PAIR = "%s %s";

    // Column name, Type pair for an SQL query.
    public final static String COLUMN_TYPE_PAIR = "%s %s";

    // No operation for SQL.
    public final static String NO_OP = "1=1";

    // The GroupBy Clause.
    public final static String SQL_GROUPBY_CLAUSE = "GROUP BY %s";

    // The Where Clause.
    public final static String SQL_WHERE_CLAUSE = "WHERE %s ";

    public static final String SQL_SELECT_QUERY =
        "SELECT %s " +  // The columns to project. (use a /n)
        "FROM %s ";     // The %s are the Tables with their aliases.

    public static final String SQL_SELECT_DISTINCT_QUERY =
    "SELECT DISTINCT %s" +  // The columns to project. (use a /n)
    "FROM %s";              // The %s are the Tables with their aliases.


    // Insert tuples into a table from a SELECT query.
    public final static String SQL_INSERT_INTO_SELECT_QUERY =
        "INSERT INTO %s\n" + // The temp table name.
        "%s";  // The Select query.


    // Insert values into a TempTable.
    public static final String SQL_INSERT_INTO_QUERY =
        "INSERT INTO %s " + // The Temp Table's name
        "VALUES %s";        // The values form the tupleSet.


    // CREATE TABLE query.
    public final static String SQL_CREATE_TABLE_QUERY =
        "CREATE TABLE %s (\n" +   // The first %s is the temp table's name
        "%s" + // The columns and their types goes here.
        "%s" + // The primary key
        ")";

    // CREATE VIEW query.
    public final static String SQL_CREATE_VIEW_QUERY =
        "CREATE VIEW %s "+   // Here goes the view name
        "AS %s \n";          // Here goes to select Query


    public final static String SQL_ADD_COLUMN_QUERY =
        "ALTER TABLE %s " +
        "ADD COLUMN %s %s";

    // This query is executed against all indexes of the database
    // and returns all tuples of a relation in which the keyword was found.
    public final static String INV_INDEX_QUERY = "SELECT %s FROM %s WHERE match(%s) against(? IN BOOLEAN MODE)";

    public final static String MYSQL_CREATE_INV_INDEX = "CREATE FULLTEXT INDEX %s ON %s(%s)";

    // This query searches for a value in a specific column
    // with the use of LIKE "%<value>%".
    public final static String LIKE_QUERY = "SELECT %s FROM %s WHERE %s LIKE ? LIMIT 2000";

    // This query searches for values that are in a specific numeric column
    // but also restricted by an operator and a number (except limit 0,5).
    public final static String NUMERIC_QUERY = "SELECT %s FROM %s WHERE %s %s ? LIMIT 5";

    // Query to get information about all columns.
    public static final String INFORMATION_SCHEMA_COLUMNS_QUERY =
    "SELECT COLUMN_NAME, TABLE_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_KEY " +
    "FROM INFORMATION_SCHEMA.COLUMNS " +
    "WHERE TABLE_SCHEMA=?";

    // Query to get information about all keys.
    public static final String INFORMATION_SCHEMA_KEY_USAGE_QUERY =
        "SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
        "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
        "WHERE TABLE_SCHEMA=? AND REFERENCED_COLUMN_NAME IS NOT NULL";

    // Query to get information about indexes.
    public static final String INFORMATION_SCHEMA_STATISTICS_QUERY =
        "SELECT TABLE_NAME, COLUMN_NAME " +
        "FROM INFORMATION_SCHEMA.STATISTICS " +
        "WHERE table_schema=? AND INDEX_TYPE='FULLTEXT'";

    // Query to get the number of rows of all tables in the schema.
    public static final String INFORMATION_SCHEMA_TABLE_ROWS_QUERY =
    "SELECT TABLE_NAME, TABLE_ROWS " +
    "FROM INFORMATION_SCHEMA.TABLES " +
    "WHERE TABLE_SCHEMA=?";

    // Query to get the average length in words of all columns with a FULLTEXT index in the current database schema.
    public static final String COLUMN_AVERAGE_LENGTH_QUERY = "SELECT * FROM avg_length";


    // Set the column containing an index. This way the index contents are available in `INNODB_FT_INDEX_TABLE`
    public static final String SET_INDEX_TABLE_FOR_SQL_TABLE = "SET GLOBAL innodb_ft_aux_table = '%s/%s'";

    public static final String GET_INDEX_SIZE = "select count(*) as cnt from (SELECT count(*) FROM INFORMATION_SCHEMA.INNODB_FT_INDEX_TABLE GROUP BY WORD ORDER BY SUM(DOC_COUNT) DESC, WORD) as a;";

    public static final String RETRIEVE_KW_FROM_IDX = "SELECT WORD, MAX(DOC_COUNT) FROM INFORMATION_SCHEMA.INNODB_FT_INDEX_TABLE GROUP BY WORD ORDER BY MAX(DOC_COUNT) %s, WORD LIMIT %s;";

    public static final String RETRIEVE_KW_FROM_IDX_WITH_OFFSET = "SELECT WORD, SUM(DOC_COUNT) FROM INFORMATION_SCHEMA.INNODB_FT_INDEX_TABLE GROUP BY WORD ORDER BY SUM(DOC_COUNT) DESC, WORD LIMIT %s,%s;";

}