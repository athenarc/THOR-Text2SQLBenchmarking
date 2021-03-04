package shared.database.model;

import java.util.ArrayList;
import java.util.List;

import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseIndexManager;


// This class models a SQL database.
public abstract class SQLDatabase {

    /**
     * This interface is needed to be implemented by the subclass.
     * SQL format does not change significantly, but inverted index queries 
     * are always different when changing implementations.
     * 
     * This interface allows users to build their inverted index condition 
     * without knowing the underlying database implementation.     
     */
    public static interface InvIdxCondBuilder {

        public InvIdxCondBuilder setColumn(String col, String TableAlias);
        public InvIdxCondBuilder setColumn(String col);
        public InvIdxCondBuilder setSearchPhrase(String phrase);

        /**
         * Builds the condition based on the functions called. The user can setColumn and setSearchPhrase.
         * 
         * @note If no searchPhrase is set then we will set "?" in its place so you can use a prepared
         * statement to set it.
         * 
         * @return The condition in string format/
         */
        public String build();

    }

    protected DatabaseType type;                             // The type of the database {psql, mysql}.
    protected String name;                                   // The database name.
    protected List<SQLTable> tables;                         // List of tables in the database.
    protected List<SQLForeignKeyConstraint> fkConstraints;   // List of foreign key constraints between tables.

    public SQLDatabase(String name) {
        this.name = name;
        this.tables = new ArrayList<SQLTable>();
        this.fkConstraints = new ArrayList<SQLForeignKeyConstraint>();
    }

    /**
     * @param types The types that we are searching for.
     * @return A List ot SQLColumns with the above types. No pks or fks are returned
     */
    public List<SQLColumn> getColumnsByType(SQLType... types) {
        List<SQLColumn> matchedColumns = new ArrayList<>(); // The Matches columns.

        // Loop all the columns in the Database.
        for (SQLColumn column : this.getAllColumns()) {
            if (column.isPrimary() || column.isForeign())
                continue; // Skip fks and pks.
            for (int i = 0; i < types.length; i++)
                if (column.getType().equals(types[i]))
                    matchedColumns.add(column);
        }

        // Return the Matches Columns.
        return matchedColumns;
    }

    public List<SQLColumn> getAllColumns() {
        List<SQLColumn> columns = new ArrayList<>();
        for (SQLTable table : this.tables)
            columns.addAll(table.getColumns());
        return columns;
    }

    // Returns the table with the given name, or null if not found.
    public SQLTable getTableByName(String tableName) {
        for (SQLTable table : this.tables) {
            if (table.getName().equals(tableName)) {
                return table;
            }
        }

        return null;
    }

    @Override
    public String toString() {
        // Add the name.
        String str = new String("Database name: " + this.name + "\n");

        // Add the tables and columns.
        for (SQLTable table : this.tables) {
            str += "\t" + table.getName() + " ( ";
            for (SQLColumn column : table.getColumns()) {
                str += column.getName() + ((column.isIndexed()) ? ("*") : ("")) + ((column.isPrimary()) ? ("^") : ("")) + " ";
            }
            str += ")\n";
        }

        // Add the constraints.
        str += "\nForeign Key constraints (PK->FK):\n";
        for (SQLForeignKeyConstraint constraint : this.fkConstraints) {
            str += "\t" + constraint.getPrimaryKeyColumn() + " --> " + constraint.getForeignKeyColumn() + "\n";
        }

        return str;
    }


    /****************************
     *  Database Action Methods *
     ****************************
     *
     * These methods are overwritten by sub classes and call different
     * objects to handle actions like: 
     * - Fill database.
     * - Create a query.
     * - Find string in indexes. 
     */


    /**
     * Instantiate a database object using the the configuration file and the database name
     * to connect to the database.
     * 
     * @param databaseName
     * @return The database object or null in case of connection error.
     */
    public static SQLDatabase InstantiateDatabase(String databaseName, DatabaseType type) {        
        DataSourceFactory.loadConnectionProperties(databaseName, type);
        
        SQLDatabase database = null;
        if (DataSourceFactory.getType().isMySQL())
            database = new MySqlDatabase(databaseName);
        else if (DataSourceFactory.getType().isPostgreSQL())
            database = new PostgreSQLDatabase(databaseName);
        else
            System.err.println("Database type not supported. Currently supporting: {Mysql, PostgreSQL}");

        // Fill the database
        if (database != null)
            database.fillDatabase();

        return database;
    }


    /**
     * Search the column's values for the appearance of a phrase. Use the database index to achieve that. 
     * Use like queries for textual attributes with less than 100 000 lines if boolean set to true.
     * 
     * @param column The column's values to search.
     * @param phrase The phrase to search.
     * @param useLike Enable searches using the like '%keyword%' where indexes are not available. Disable it for faster lookups.
     * @return
     */
    public SQLIndexResult searchColumn(SQLColumn column, String phrase, boolean useLike) {
        return DatabaseIndexManager.searchKeyword(this, phrase, column, useLike);
    }


    /**     
     * @return A {@link Query} to build a query.
     */
    public Query getQuery() {
        return new Query(this);
    }
    

    /**
     * Fills the database object automatically by reading information
     * from the Database Engine (Mysql/postgre).
     */
    public abstract void fillDatabase();
    


    /**
     * Return a parametrized string for the grammar of querying a database index.
     * The parametrized string should take 2 string parameters, the columns and the keyword.
     */
    public abstract InvIdxCondBuilder getInvIndexCondition();


    /**
     * Prepares the phrase, if it contains more than one words, for an
     * "AND" full text search. This means that the results of the search must 
     * contain all words in the phrase.     
     */
    public abstract String prepareForAndFullTextSearch(String phrase);



    /***********************
     *  Getters And Setter *
     ***********************/

    /**
     * @return the type
     */
    public DatabaseType getType() {
        return type;
    }

    /**
     * @return the type
     */
    public void setType(DatabaseType type) {
        this.type = type;
    }

    // Getters and Setters.
    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<SQLTable> getTables() {
        return this.tables;
    }

    public void setTables(List<SQLTable> tables) {
        this.tables = tables;
    }

    public List<SQLForeignKeyConstraint> getFKConstrains() {
        return this.fkConstraints;
    }

    public void setFKConstrains(List<SQLForeignKeyConstraint> fkConstraints) {
        this.fkConstraints = fkConstraints;
    }

    // Adds a new table.
    public void addTable(SQLTable table) {
        this.tables.add(table);
    }

    // Adds a new constraint.
    public void addFKConstraint(SQLForeignKeyConstraint constraint) {
        this.fkConstraints.add(constraint);
    }

    // Returns true if the database object is empty.
    public boolean isEmpty() {
        return this.tables.isEmpty();
    }

    // Return ta column by it's name
    public SQLColumn getColumnByName(String columnName) {
        for (SQLTable table: this.tables) {
            SQLColumn col = table.getColumnByName(columnName);
            if (col != null)
                return col;
        }
		return null;
	}

}
