package discoverIR.components.execution.executors;

import java.util.List;

import discoverIR.components.execution.ExecutionPreProcessor;
import discoverIR.exceptions.ShutdownHook;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import discoverIR.model.TupleSet;

// 
public abstract class Executor {
    // Represents a equation used to join two tables.
    //     [table_name].[pk_col_name] = [table_name].[fk_col_name]
    protected class JoinEquation {

        String lhs; // Its the part : [table_name].[pk_col_name].
        String rhs; // Its the part : [table_name].[fk_col_name].

        JoinEquation(SQLColumn pkColumn, SQLColumn fkColumn) {
            this.lhs = pkColumn.getTableName() + "." + pkColumn.getName();
            this.rhs = fkColumn.getTableName() + "." + fkColumn.getName();
        }

        @Override
        public int hashCode() {
            return lhs.hashCode() + rhs.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) return true;
            if (obj == null) return false;
            if (!(obj instanceof JoinEquation)) return false;
            JoinEquation je = (JoinEquation) obj;
            return (lhs.equals(je.lhs) && rhs.equals(je.rhs)) ||
                (lhs.equals(je.rhs) && rhs.equals(je.lhs));
        }

        @Override
        public String toString() {
            return this.lhs + " = " + this.rhs;
        }

    }

    protected SQLDatabase database; // The database instance.    
    protected SchemaGraph schemaGraph; // The schema graph.
    protected List<SQLColumn> columnsToSelect; // columns to select in each parameterized query.    
    protected List<TupleSet> nonFreeTupleSets; // A list of all non free tuple sets used in the execution plan.    
    protected Integer maxTuples; // The max number of tuples as a result.

    private ShutdownHook shutdownHook; // The shutdown hook

    // The executor's PreProcessor.
    protected ExecutionPreProcessor preProcessor;

    // Public Constructor.
    public Executor(
        SQLDatabase database,
        SchemaGraph schemaGraph,
        Integer maxTuples,
        List<TupleSet> nonFreeTupleSets) 
    {
        this.database = database;        
        this.maxTuples = maxTuples;
        this.schemaGraph = schemaGraph; // Copy the schema graph.
        this.preProcessor = new ExecutionPreProcessor(nonFreeTupleSets, database);
        this.shutdownHook = new ShutdownHook(this.preProcessor);
        Runtime.getRuntime().addShutdownHook(this.shutdownHook);
    }

    // Initialize the Executor. This initialization includes
    // tempTable creation for each nonFreeTupleSet created by the IREngine.
    public void initializeExecutor() {
        // Create a temp table for each Tuple Set, update the database instance and the modified Schema graph.
        this.preProcessor.createTempTablesForTupleSets();
    }

    // Restores all changes created to the TupleSets and SQLTables and
    // drop all the temporary Tables Created.
    public void finalizeExecutor() {
        this.preProcessor.restoreChanges();
        this.preProcessor.dropAllTempTables();
        Runtime.getRuntime().removeShutdownHook(this.shutdownHook);
    }
}