package discoverIR.components.execution;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import discoverIR.DiscoverIRApplication;
import discoverIR.components.SQLQueryCreator;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.SQLTempTable;
import discoverIR.model.TupleSet;
import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLQueries;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLTable;
import shared.util.Timer;

// The main functionality of the ExecutionPreProcessor is
// to create a new Temporary Table for each Tuple Set that
// used in the Execution Plan. The Original SQLTables that 
// each TupleSet uses till this step of execution contain 
// all the tuples that the instance of The database contains.
// The Temp Tables we are about to create contain only 
// the tuples matching a keyword of the query along with 
// each tuples Score for the query.
public class ExecutionPreProcessor {
    public static String AUX_DB = PropertiesSingleton.getBundle("app").getString("database.mysql.auxDatabase");    
    
    private int tempTablesNum;  // An integer indicating the number of temp tables created.    
    private static int batchCardinality = 50;
    private List<TupleSet> tupleSets; // The list of tupleSets.
    private List<SQLTable> oldTupleSetsTables; // The old Tables of the tuples sets. (before creating temp Tables)
    
    // A database instance holding all the new temp tables
    // that we will add in the ExecutorPreProcessing step.
    private SQLDatabase modifiedDatabase;

    // Store the tempTables.
    private List<SQLTempTable> tempTablesList;

    // Statistics
    private double timeCreatingTempTables;

    public ExecutionPreProcessor(List<TupleSet> tupleSets, SQLDatabase database) {
        this.tupleSets = tupleSets;
        this.modifiedDatabase = database;
        this.oldTupleSetsTables = new ArrayList<>();
        this.tempTablesList = new ArrayList<>();
        this.tempTablesNum = 0;

        // Keep a List of SQLTables of tupleSet, because at the end of the
        // execution we will need to restore them.
        for (TupleSet tupleSet: this.tupleSets) {            
            this.oldTupleSetsTables.add(tupleSet.getTable());
        }
    }

    // Create a new temporary table for each Tuple Set used in this
    // execution plan. Each temp table contains only those tuples 
    // who contained a part of the query along with their Scores. 
    // We Use this Tables instead of the original SQLTable.
    public void createTempTablesForTupleSets() {
        // Start the timer.
        Timer timer = new Timer();
        timer.start();

        // For all Tuple Sets create a temporary Table.
        for (TupleSet tupleSet: this.tupleSets) {
            SQLTempTable tempTable = createTempTable(tupleSet);            
            
            // Replace the original SQLTable with the temp Table.
            tupleSet.setTable(tempTable);
            
            // Replace the columns Containing keywords with the new tables columns.
            Set<SQLColumn> columnSet = new HashSet<>();
            for (SQLColumn column: tupleSet.getColumnsContainingKeywords()) {
                columnSet.add(tempTable.getColumnByName(column.getName()));
            }
            tupleSet.setColumnsContainingKeywords(columnSet);
        }
        
        // Stop the timer.
        this.timeCreatingTempTables = timer.stop();
    }

     // Creates the SQLTempTable and runs a CREATE_TABLE query
    // against the database.
    private SQLTempTable createTempTable(TupleSet tupleSet) {
        // Create the temp Tables name. It will contain the 
        String tempTableName = AUX_DB + ".discoverIR_temp" + this.tempTablesNum++ + "_" + tupleSet.getTable().getName();        

        // Crete a temp table Like the tupleSets Table.
        SQLTempTable tempTable = new SQLTempTable(tempTableName);
        tempTable.fill(tupleSet);        
        this.modifiedDatabase.addTable(tempTable);  // Update the database.
        this.tempTablesList.add(tempTable);         // Update the temp table list.

        // Create the CREATE_TABLE query.
        String createTableQuery = SQLQueryCreator.createSQLCreateTableQuery(tempTable);

        // Create the INSERT_INTO_SELECT query.
        List<String> insertIntoQueries = this.createBatchesOfInsertIntoQueries(
            ExecutionPreProcessor.batchCardinality,
            tupleSet.getTuples(),
            tempTable
        );
        
        if (DiscoverIRApplication.DEBUG_PRINTS)
            System.out.println("Crete query:\n" + createTableQuery);        

        // Initialize connection variables.
        Connection con = null;
        Statement stmt = null;
        
        // Execute the two update queries.
        try {
            con = DataSourceFactory.getConnection();
            stmt = con.createStatement();

            // Execute the create query.
            stmt.executeUpdate(createTableQuery);
            // Temporary disable the Constraints.
            stmt.executeUpdate(SQLQueries.SQL_DISABLE_CONSTRAINTS_QUERY);
            // Execute the insert into.
            this.executeInsertIntoQueryInBatches(stmt, insertIntoQueries);       
            // Enable the constraints;
            stmt.executeUpdate(SQLQueries.SQL_ENABLE_CONSTRAINTS_QUERY);                    
        } 
        catch (SQLException e) {
           e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt);
        }

        return tempTable;
    }   


    // Drops all temp tables that the Execution created (if any created).
    public void dropAllTempTables() {
        // If there are none return.
        if (tempTablesList.isEmpty()) return;        

        // Initialize connection variables.
        Connection con = null;
        Statement stmt = null;

        // Create a string containing all temp Tables names 
        String listOfTempTables = new String();
        for (SQLTempTable tempTable: tempTablesList) {
            listOfTempTables += tempTable.getName() + ", ";
        }
        // Remove the last ", ";
        listOfTempTables = listOfTempTables.substring(0, listOfTempTables.length()-2);        
        
        try {
            // Connect with the database
            con = DataSourceFactory.getConnection();
            stmt = con.createStatement();

            // Temporary disable the Constraints.
            stmt.executeUpdate(SQLQueries.SQL_DISABLE_CONSTRAINTS_QUERY);
            // Drop a list of temp tables.
            stmt.executeUpdate(String.format(SQLQueries.SQL_DROP_TABLES_QUERY, listOfTempTables));
            // Temporary enable the Constraints.
            stmt.executeUpdate(SQLQueries.SQL_ENABLE_CONSTRAINTS_QUERY);
        } 
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt);
        }

        // Clear the TempTables from the database array.
        this.modifiedDatabase.getTables().removeAll(this.tempTablesList);
        this.tempTablesNum = 0;
    }


    // Create InsertIntoValues queries to insert the parameter tuples to the 
    // tempTable where each query will insert batchCardinality number of tuples in the table. 
    private List<String> createBatchesOfInsertIntoQueries(
        int batchCardinality, 
        List<OverloadedTuple> tuples,
        SQLTempTable tempTable)
    {
        List<String> queries = new ArrayList<>();

        // Split tuples to batches of batchCardinality.
        List<List<OverloadedTuple>> batchesOfTuples = new ArrayList<>();

        // Initialize the list of the first batch.
        batchesOfTuples.add(new ArrayList<>());

        // Loop tuples to split them.
        int tupleNumber = 0;
        for (OverloadedTuple tuple: tuples) {
            // Add the tuple to the last list of tuples.
            batchesOfTuples.get(batchesOfTuples.size()-1).add(tuple);
            tupleNumber++;

            // Create a new list 
            if (tupleNumber == batchCardinality) {
                tupleNumber = 0;
                batchesOfTuples.add(new ArrayList<>());
            }
        }

        // Check if last batch is empty and remove it.
        if (batchesOfTuples.get(batchesOfTuples.size() -1).isEmpty())
            batchesOfTuples.remove(batchesOfTuples.size() -1);

        // Create the queries.
        for (int batch = 0; batch < batchesOfTuples.size(); batch++) {
            queries.add(SQLQueryCreator.createSQLInsertIntoValuesQuery(
                batchesOfTuples.get(batch), tempTable)
            );
        }        

        return queries;
    }

    // Execute a list of SQL INSERT INTO queries.
    private void executeInsertIntoQueryInBatches(Statement stmt, List<String> queries) throws SQLException {                
        for (String query: queries) {
            // if (DiscoverIRApplication.DEBUG_PRINTS)
            //     System.out.println("Insert into:\n" + query);
            stmt.executeUpdate(query);
        }
    }   

    // Restores the changes that this class made to the
    // tuple sets and SQLTables.
    public void restoreChanges() {
        // But back the original tables in the TupleSets.
        for (int index = 0; index < this.tupleSets.size(); index++) {                                    
            this.tupleSets.get(index).setTable(this.oldTupleSetsTables.get(index));

            // Replace the columns Containing keywords with the original tables columns.
            Set<SQLColumn> columnSet = new HashSet<>();
            for (SQLColumn column: this.tupleSets.get(index).getColumnsContainingKeywords()) {
                columnSet.add(this.oldTupleSetsTables.get(index).getColumnByName(column.getName()));
            }
            this.tupleSets.get(index).setColumnsContainingKeywords(columnSet);
        }
    }

    public List<SQLTempTable> getTempTablesList() {
        return this.tempTablesList;
    }


    // Print the statistics
    public void printStats() {
        System.out.println("EXECUTION PRE PROCESSOR STATS:");
        System.out.println("\tTime to execute the Plan: " + this.timeCreatingTempTables);
    }

}

