package discoverIR.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import discoverIR.DiscoverIRApplication;
import discoverIR.components.Joiner;

import shared.database.model.SQLColumn;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.database.model.graph.SchemaGraph;
import discoverIR.exceptions.JoinCandidateNotFoundException;
import discoverIR.model.FreeTupleSet;
import discoverIR.model.TupleSet;
import shared.util.Pair;


// The Joinable Format models a Format tha can be easily transformed to any SQL Query.
// It stores information that are important for creating the SQLQuery.
public class JoinableFormat {
    private List<Pair<SQLTable, String>> tablesToJoin;         // The tables to Join along with their aliases
    private List<Pair<SQLColumn, String>> columnsFromTables;   // Useful columns from the above tables with their aliases.
    private List<JoinEquation> joinEquations;                  // The equations used to Join the above tables.

    /* Optional Fields */

    // Depending on the objects that fill the JoinableFormat, create a list of strings that
    // stores IN LIST SQL Constraints. (Happens for TupleSets)
    private List<String> inListConstraints;

    // If the Tables Used to Join contained score columns keep them in a separate list.
    // This is necessary because we will group them in the query as one score column 
    // summing all the columns from this list.
    private List<Pair<SQLColumn, String>> scoreColumns;


    // The public Constructor.
    public JoinableFormat() {
        this.tablesToJoin = new ArrayList<>();
        this.columnsFromTables = new ArrayList<>();
        this.joinEquations = new ArrayList<>();
        
        this.inListConstraints = new ArrayList<>();
        this.scoreColumns = new ArrayList<>();
    }

    // Getters and Setters.
    public List<Pair<SQLTable, String>> getTablesToJoin() { return tablesToJoin;}    
    public List<Pair<SQLColumn, String>> getColumnsFromTables() { return columnsFromTables; }            
    public List<Pair<SQLColumn, String>> getScoreColumns() { return scoreColumns; }    
    public List<JoinEquation> getJoinEquations() { return joinEquations; }
    public List<String> getInListConstraints() { return inListConstraints; }    


    // Get ColumnsFromTables in a list without aliases.    
    public List<SQLColumn> getColumnsWithoutAliases() {
        List<SQLColumn> columns = new ArrayList<>();
        for(Pair<SQLColumn, String> pair: this.columnsFromTables) {
            columns.add(pair.getLeft());
        }

        // if there are score columns add another column that represents the sum 
        // of the score columns, a mock SQLColumn.     
        if (!scoreColumns.isEmpty())   
            columns.add(new SQLColumn(null, "score", new SQLType("double", 0), ""));
        
        return columns;
    }    

    // Return the Table, Table Alias pair for the SQLTable table.    
    // Return null if table not found.
    public Pair<SQLTable, String> getPairForTable(SQLTable table) {
        Pair<SQLTable, String> lastPair = null;

        // Loop the Tables and find the column's Table
        for (Pair<SQLTable, String> pair: this.tablesToJoin)
            if (pair.getLeft().equals(table)) 
                lastPair = pair;
                
        // Return the lastIndexAlias of the Table.
        return lastPair;
    }


    // Fill JoinableFormat from a CandidateNetwork
    public void fill(
        JoiningNetworkOfTupleSets candidateNetwork,
        SchemaGraph schemaGraph)
            throws JoinCandidateNotFoundException 
    {
        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.        
        List<Pair<TupleSet, String>> tupleSets = new ArrayList<>();
        Integer aliasesCount = 0;

        // Create an alias for the root's table and add it this.tablesToJoin array.
        // Also store the tupleSet along with the root's Alias.
        TupleSet rooTupleSet = candidateNetwork.getRoot().getTupleSet();
        String rootAlias = SQLQueries.ALIAS_LETTER + aliasesCount;

        this.tablesToJoin.add ( new Pair<>(rooTupleSet.getTable(), rootAlias) );
        tupleSets.add ( new Pair<>(rooTupleSet, rootAlias) );                
        aliasesCount++; // Increment aliases count.

        // Add root to the queue and start traversing the Candidate Network.
        queue.add(candidateNetwork.getRoot());
        while (!queue.isEmpty()) {
            Node parent = queue.remove(); // Remove the parent node.

            // Create an equation parent.column = child.column for each of the parents children.
            // This equation will be used in the WHERE part of the query to specify the joins.
            for (Node child : parent.getChildren()) {

                // Create the child's table and alias.
                Pair<SQLTable, String> childPair = new Pair<>(child.getTupleSet().getTable(), SQLQueries.ALIAS_LETTER + aliasesCount);
                this.tablesToJoin.add(childPair);
                tupleSets.add ( new Pair<>(child.getTupleSet(), SQLQueries.ALIAS_LETTER + aliasesCount) );
                aliasesCount++; // Increment aliases count.

                // Get the parent's Table and Table Alias pair.                
                Pair<SQLTable, String> parentPair = this.getPairForTable(parent.getTupleSet().getTable());

                // Get the join Equation Joining child and parent Tables.
                this.joinEquations.add( Joiner.getJoinEquation(parentPair, childPair, schemaGraph) );                

                // Add the child to the queue
                queue.add(child);
            }
        }     

        // Now use the Tables created to fill some more fields.
        this.fillColumnsFromTables();
        this.fillInListConstraints(tupleSets);        
    }   

    // Fill the HashSet columnsFromTable from the HashMap TablesToJoin.
    // A useful column is a column that is part of the PK or an FK of a table.
    // Also columns that store textual attributes and have FULLTEXT indexes 
    // are also picked as useful columns.
    public void fillColumnsFromTables() {
        // Loop all the Tables.        
        for (Pair<SQLTable, String> pair: this.tablesToJoin) {
            SQLTable table = pair.getLeft();                       // The table to process.
            HashSet<SQLColumn> pksAndFksUsed  = new HashSet<>();   // A set keeping the PKs and FKs so we dont reuse an fk also being part of the pk
            List<SQLColumn> orderedColumns   = new ArrayList<>();  // The columns to select.

            // Pick the Foreign Keys and the Primary Key first.
            for (SQLColumn pk: table.getPrimaryKey()) {
                if (!pksAndFksUsed.contains(pk)) {
                    orderedColumns.add(pk);
                    pksAndFksUsed.add(pk);
                }
            }
            for (SQLColumn fk: table.getForeignKeys()) {
                if (!pksAndFksUsed.contains(fk)) {
                    orderedColumns.add(fk);
                    pksAndFksUsed.add(fk);
                }
            }            

            // Pick the TextualColumns that are indexed.
            for(SQLColumn column: table.getColumns())
                if (!pksAndFksUsed.contains(column))
                    if (column.getType().isTextual()) // if (column.isIndexed() && column.getType().isTextual())
                    orderedColumns.add(column);                                 
            
            // Pick the score Column.
            SQLColumn scoreColumn = table.getColumnByName("score");
            if (scoreColumn != null)
                this.scoreColumns.add(new Pair<>(scoreColumn, pair.getRight()));            

            // Add all the selected columns to the general List.
            for (SQLColumn column: orderedColumns)
                this.columnsFromTables.add(new Pair<>(column, pair.getRight()));
        }
    }     

    // Fill the List<String> inListConstraints storing IN LIST SQLConstraints for the 
    // SQLTables and their tuples. Because we have to join TupleSets we need to pick
    // specific tuples from the SQLTables connected with them. The parameter 
    // tupleSets indicate how to fill the List of inListConstraints.
    public void fillInListConstraints(List<Pair<TupleSet,String>> tupleSets) {        
        // Loop all the TupleSets.
        for (Pair<TupleSet, String> pair: tupleSets) {
            // If a tupleSet's table is created by the pre processor as a temp Table then dont create
            // an in list Constraint. If we use GlobalPiplined then skip this if, because we will need to 
            // get part of table tables (prefixes).
            if (
                DiscoverIRApplication.parameters.executionEngineAlgorithm != ExecutionEngineAlgorithms.GlobalPipelined &&
                pair.getLeft().getTable() instanceof SQLTempTable
            ) 
                continue;
            String inListConstraint = this.getInListConstraintForIdsOfTupleSet(pair.getLeft(), pair.getRight());
            if (inListConstraint != null)
                this.inListConstraints.add(inListConstraint);
        }
    }

    // Returns an IN LIST SQLConstraint in String format, containing primaryKey values of the tuples
    // inside the parameter tupleSet. This IN LIST SQLConstraint is formated like this :
    //      [table_identifier].[primaryKeyColumn] IN LIST ([ColumnValue], ..., [ColumnValue])
    // Remember that each tupleSet is connected with an SQLTable so the primaryKeys
    // and the identifier is borrowed by that SQLTable.
    protected String getInListConstraintForIdsOfTupleSet(TupleSet tupleSet, String tableAlias) {
        // Dont process FreeTupleSets, they dont have tuples.
        if (tupleSet instanceof FreeTupleSet) return null;
        if (tupleSet.isEmpty()) return null;
        
        // The String storing the in list assignment.
        String inListAssignment = new String();        

        // Get the primary key column of the table connecting with this tupleSet.
        Set<SQLColumn> primaryKey = new HashSet<>(tupleSet.getTuples().get(0).getPrimaryKeys());

        // Get the TupleSets tuples and extract the value of the PK columns.
        for (SQLColumn pkColumn: primaryKey) {
            // Get the values of the pkColumn
            String idList = new String();            
            for (SQLTuple tuple: tupleSet.getTuples()) {
                SQLValue value = tuple.getValueOfColumn(pkColumn);
                if (value != null) {
                    idList += value.toString() + ", ";
                }
            }            

            // Remove the last ", ".
            if (idList.length() != 0)
                idList = idList.substring(0, idList.length()-2);
            
            // Create the assignment
            String columnWithAlias = tableAlias + "." + pkColumn.getName();
            inListAssignment += String.format(
                SQLQueries.IN_LIST_CONSTRAINT,
                columnWithAlias, idList)
                + " AND ";
        }

        // Remove the last " AND ".
        if (!inListAssignment.isEmpty())
            inListAssignment = inListAssignment.substring(0, inListAssignment.length() - 5);
        

        return inListAssignment;
    }    
     
}
