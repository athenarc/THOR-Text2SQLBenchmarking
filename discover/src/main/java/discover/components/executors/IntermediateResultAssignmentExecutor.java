package discover.components.executors;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import discover.DiscoverApplication;
import discover.components.ExecutionPreProcessor;
import discover.components.SQLQueryCreator;
import discover.exceptions.JoinCandidateNotFoundException;
import discover.model.SQLTempTable;
import discover.model.execution.IntermediateResultAssignment;
import discover.model.execution.JoinableFormat;
import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;

// Input : An Intermediate Result Creation Assignment.
// Output : None.
//
// The IntermediateResultAssignmentExecutor executes two queries against the 
// database. The firs is a CREATE TABLE query creating the a temporary table 
// for storing the Intermediate Result's tules. Then an INSERT INTO query is 
// executed filling the temporary with the Intermediate Result's tuples.
public class IntermediateResultAssignmentExecutor {


    // public static String AUX_DB = null;
    public static String AUX_DB = PropertiesSingleton.getBundle().getString("database.auxDatabase");
    public static String VIEW_PREFIX = AUX_DB + ".DView_";
    

    public static void execute(IntermediateResultAssignment assignment, ExecutionPreProcessor preProcessor,
            SchemaGraph schemaGraph, SQLDatabase database) throws JoinCandidateNotFoundException {
        if (DiscoverApplication.USE_VIEWS)
            IntermediateResultAssignmentExecutor.executeVIEW(assignment, preProcessor, schemaGraph, database);
        else
            IntermediateResultAssignmentExecutor.executeTABLE(assignment, preProcessor, schemaGraph, database);
    }

    // Executes an intermediate assignment.
    // Nothing is returned, only a temporary table is inserted to the database.    
    public static void executeTABLE(IntermediateResultAssignment assignment, ExecutionPreProcessor preProcessor,
            SchemaGraph schemaGraph, SQLDatabase database) throws JoinCandidateNotFoundException {
        // Create a generic joinable format from the assignments Terms.
        JoinableFormat joinableFormat = new JoinableFormat();
        joinableFormat.fill(assignment.getAssignmentTerms(), schemaGraph);

        // Create a new TempTable and fill it with the joinableFormat.
        String tempTableName = VIEW_PREFIX + assignment.toAbbreviation();        
        SQLTempTable tempTable = new SQLTempTable(tempTableName);
        tempTable.fill(joinableFormat);        

        // Create an SQLCreateTable Query for the tempTable.
        String tempTableCreationQuery = SQLQueryCreator.createSQLCreateTableQuery(tempTable);

        // Create an SQL INSERT INTO SELECT query filling the tempTable with the data of the joinable format.
        String insertIntoQuery = SQLQueryCreator.createSQLInsertIntoSelectQuery(joinableFormat, tempTable);
        
        // Debug prints
        if (DiscoverApplication.DEBUG_PRINTS) {
            System.out.println("Creation Query:\n" + tempTableCreationQuery + "\n");
            System.out.println("InsertInto Query :" + insertIntoQuery + "\n");
        }
               
        // Synchronize this part of the code using a lock from ExecutionPreProcessor
        synchronized(ExecutionPreProcessor.class) {
            // Run the queries against MysqlServer.
            Connection con = null;
            Statement stmt = null;
            try {
                // Get the connection.
                con = DataSourceFactory.getConnection();
                stmt = con.createStatement();
                
                // Use the statement to submit the CREATE TABLE query.
                stmt.executeUpdate(tempTableCreationQuery);   

                // Use the same statement again to submit the INSERT INTO SELECT query.        
                stmt.executeUpdate(insertIntoQuery);
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            finally {
                DatabaseUtil.close(con, stmt);
            }

            // Update the database with the tempTable.
            database.addTable(tempTable);
            ExecutionPreProcessor.getTempTablesList().add(tempTable);
        }

        // Update the assignmentTerms of this assignment that they are now created by a tempTable.
        assignment.getAssignmentTerms().setTable(tempTable);
    }

    // Executes an intermediate assignment.
    // Nothing is returned, only a temporary table is inserted to the database.    
    public static void executeVIEW(IntermediateResultAssignment assignment, ExecutionPreProcessor preProcessor,
            SchemaGraph schemaGraph, SQLDatabase database) throws JoinCandidateNotFoundException {
        // Create a generic joinable format from the assignments Terms.
        JoinableFormat joinableFormat = new JoinableFormat();
        joinableFormat.fill(assignment.getAssignmentTerms(), schemaGraph);
  

        // Create a new TempTable and fill it with the joinableFormat.
        String tempTableName = VIEW_PREFIX + assignment.toAbbreviation();
        SQLTempTable tempTable = new SQLTempTable(tempTableName);
        tempTable.fill(joinableFormat);

        // Create an SQLView Query for the tempTable.
        String tempViewQuery = SQLQueryCreator.createViewQuery(joinableFormat, tempTable);

        // Debug prints
        if (DiscoverApplication.DEBUG_PRINTS)
            System.out.println("View Query :\n" + tempViewQuery + "\n");
               
        // Run the queries against MysqlServer.
        Connection con = null;
        Statement stmt = null;
        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();
            stmt = con.createStatement();
            
            // Use the statement to submit the CREATE TABLE query.
            stmt.executeUpdate(tempViewQuery);               
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt);
        }

        // Update the database with the tempTable.
        database.addTable(tempTable);
        preProcessor.getTempTablesList().add(tempTable);

        // Update the assignmentTerms of this assignment that they are now created by a tempTable.
        assignment.getAssignmentTerms().setTable(tempTable);
    }

}