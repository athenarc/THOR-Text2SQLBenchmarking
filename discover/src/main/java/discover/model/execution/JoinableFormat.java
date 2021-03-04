package discover.model.execution;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import discover.model.Node;
import discover.DiscoverApplication;
import discover.components.Joiner;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLColumn;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import shared.database.model.SQLValue;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;

import discover.model.SQLTempTable;
import discover.exceptions.JoinCandidateNotFoundException;
import discover.model.FreeTupleSet;
import discover.model.JoinableExpression;
import discover.model.JoiningNetworkOfTupleSets;
import discover.model.TupleSet;


// The Joinable Format models a Format tha can be easily transformed to any SQL Query.
// It stores information that are important for creating the SQLQuery.
public class JoinableFormat {
    private List<Pair<SQLTable, String>> tablesToJoin;        // The tables to Join along with their aliases
    private List<Pair<SQLColumn, String>> columnsFromTables;  // Useful columns from the above tables with their aliases.
    private List<JoinEquation> joinEquations;                 // The equations used to Join the above tables.

    /* Optional Fields */
    // Depending on the objects that fill the JoinableFormat, create a list of strings that
    // stores IN LIST SQL Constraints. (Happens for TupleSets)
    private List<String> inListConstraints;
    private List<String> valueConstraints;

    // I know that its hard coded bu there is no time and these are too complicated to change.
    static final String INV_INDEX_CONSTRAINT_MYSQL = "MATCH(%s) AGAINST('%s' in boolean mode)";
    static final String INV_INDEX_CONSTRAINT_PSQL = "%s @@ to_tsquery('%s')";    


    // The public Constructor.
    public JoinableFormat() {
        this.tablesToJoin = new ArrayList<>();
        this.columnsFromTables = new ArrayList<>();
        this.joinEquations = new ArrayList<>();
        this.inListConstraints = new ArrayList<>();
        this.valueConstraints = new ArrayList<>();
    }

    // Getters and Setters.
    public List<Pair<SQLTable, String>> getTablesToJoin() { return tablesToJoin;}    
    public List<Pair<SQLColumn, String>> getColumnsFromTables() { return columnsFromTables; }        
    public List<JoinEquation> getJoinEquations() { return joinEquations; }
    public List<String> getInListConstraints() { return inListConstraints; }  
    public List<String> getValueConstraints() { return valueConstraints; }  
      

    // Get ColumnsFromTables in a list without aliases.
    public List<SQLColumn> getColumnsWithoutAliases() {
        List<SQLColumn> columns = new ArrayList<>();
        for(Pair<SQLColumn, String> pair: this.columnsFromTables) {
            columns.add(pair.getLeft());
        }
        return columns;
    }

    // Return the the table alias for the SQLColumn's Table.
    // Return null if table not found.
    public String getAliasForTable(SQLTable table) {
        // Loop the Tables and find the column's Table
        for (Pair<SQLTable, String> pair: this.tablesToJoin)
            if (pair.getLeft().equals(table)) 
                return pair.getRight();
        // If not found return null
        return null;
    }


    // Fill JoinableFormat from a JoinableExpression
    public void fill(JoinableExpression joinableExpression, SchemaGraph schemaGraph) {        
        List<Pair<TupleSet, String>> tupleSets = new ArrayList<>();

        // Create the JoinEquations for the joinableExpression.
        try {
            this.joinEquations = Joiner.getEquationsForJoinableExpression(
                joinableExpression, schemaGraph, tupleSets, this.tablesToJoin
            );
        } catch (JoinCandidateNotFoundException e) {
            e.printStackTrace();
        }

        // Fill the value constraints OR in list constraints.
        if (DiscoverApplication.USE_VALUE_CONST)
            this.fillValueConstraints(tupleSets);
        else                
            this.fillInListConstraints(tupleSets);
        

        // Fill the columns from the Tables.
        this.fillColumnsFromTables();        
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

        // Fill the value constraints OR in list constraints.        
        if (DiscoverApplication.USE_VALUE_CONST)
            this.fillValueConstraints(tupleSets);
        else                
            this.fillInListConstraints(tupleSets);

        // Now use the Tables created to fill some more fields.
        this.fillColumnsFromTables();        
    }  

    // Fill the columnsFromTables list with columns from the tables in tablesToJoin list.
    // A useful column is a column that is part of the PK or an FK of a table.
    // Also columns that store textual attributes and have FULLTEXT indexes 
    // are picked as useful columns.
    public void fillColumnsFromTables() {
        // Loop all the Tables.
        for (Pair<SQLTable, String> pair: this.tablesToJoin) {
            SQLTable table = pair.getLeft();
            HashSet<SQLColumn> pksAndFksUsed = new HashSet<>();
            List<SQLColumn> orderedColumns   = new ArrayList<>();

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
                // if (column.getType().isTextual()) // if (column.isIndexed() && column.getType().isTextual())
                    orderedColumns.add(column);                                    

            // Add all the selected columns to the general List.
            for (SQLColumn column: orderedColumns)
                this.columnsFromTables.add(new Pair<>(column, pair.getRight()));
        }
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

    // Fill the List<String> inListConstraints storing IN LIST SQLConstraints for the 
    // SQLTables and their tuples. Because we have to join TupleSets we need to pick
    // specific tuples from the SQLTables connected with them. The parameter 
    // tupleSets indicate how to fill the List of inListConstraints.
    public void fillInListConstraints(List<Pair<TupleSet,String>> tupleSets) {        
        // Loop all the TupleSets.
        for (Pair<TupleSet, String> pair: tupleSets) {
            // If a tupleSet's table is created by the pre processor as a temp Table then dont create
            // an in list Constraint.
            if (pair.getLeft().getTable() instanceof SQLTempTable) continue;
            String inListConstraint = this.getInListConstraintForIdsOfTupleSet(pair.getLeft(), pair.getRight());
            if (inListConstraint != null && !inListConstraint.isEmpty())
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

        // Get the primary key column of the table connecting with this tupleSet.
        Set<SQLColumn> primaryKey = tupleSet.getTable().getPrimaryKey();        

        // Get the TupleSets tuples and extract the value of the PK columns.
        String columnsWithAliases = new String();
        String columnValues = new String();

        if (primaryKey.size() > 1)  {
            // Create columns with aliases
            columnsWithAliases += "(";
            for (SQLColumn pkColumn: primaryKey)
                columnsWithAliases += tableAlias + "." + pkColumn.getName() + ", ";
            columnsWithAliases = columnsWithAliases.substring(0, columnsWithAliases.length() - 2) + ")";

            // Create column values
            for (SQLTuple tuple: tupleSet.getTuples()) {

                // Create (pkColVal1, pkColVal2, ..), (pkColVal1, pkColVal2, ..), ..
                columnValues += "(";
                for (SQLColumn pkColumn: primaryKey) {
                    SQLValue value = tuple.getValueOfColumn(pkColumn);
                    if (pkColumn.getType().isArithmetic())
                        columnValues += value.toString() + ", ";
                    else 
                        columnValues += "'" + DatabaseUtil.escapeStrValue(value.toString()) + "', ";
                }
                columnValues = columnValues.substring(0, columnValues.length() - 2) + "), ";
            }

            // Remove the last ", ".
            columnValues = columnValues.substring(0, columnValues.length() - 2);
        }
        else {
            SQLColumn pkc = primaryKey.iterator().next();
            columnsWithAliases += tableAlias + "." + pkc.getName();

            // Create the columnValues
            for (SQLTuple tuple: tupleSet.getTuples()) {
                SQLValue value = tuple.getValueOfColumn(pkc);
                if (pkc.getType().isArithmetic())
                    columnValues += value.toString() + ", ";
                else 
                    columnValues += "'" + DatabaseUtil.escapeStrValue(value.toString()) + "', ";
            }

            // Remove the last ", ".
            columnValues = columnValues.substring(0, columnValues.length()-2);
        }

        // Create the assignment
        return String.format(
            SQLQueries.IN_LIST_CONSTRAINT,
            columnsWithAliases, columnValues
        );
    }   
    
    

    public void fillValueConstraints(List<Pair<TupleSet,String>> tupleSets) {        
        // Loop all the TupleSets.
        for (Pair<TupleSet, String> pair: tupleSets) {

            // If a tupleSet's table is created by the pre processor as a temp Table then dont create an in list Constraint.
            if (pair.getLeft().getTable() instanceof SQLTempTable) continue;
            String valueConstraints = this.getValueConstraintFromTupleSet(pair.getLeft(), pair.getRight());
            if (valueConstraints != null && !valueConstraints.isEmpty())
                this.valueConstraints.add(valueConstraints);
        }
    }


    protected String getValueConstraintFromTupleSet(TupleSet tupleSet, String tableAlias) {
        // Dont process FreeTupleSets, they dont have tuples.
        if (tupleSet instanceof FreeTupleSet) return null;

        // Tuples created as TempTables dont have already their keywords
        if (tupleSet.getTable() instanceof SQLTempTable) return null;
        if (tupleSet.getKeywords2columns().isEmpty()) return null;

        String constraints = new String();
        
        // Create a match against constraint for each keyword 
        for (Map.Entry<String, Set<SQLColumn>> entry: tupleSet.getKeywords2columns().entrySet()) {
            constraints += "( ";
            for (SQLColumn col: entry.getValue()) {

                constraints += String.format(  
                    (DataSourceFactory.getType().isMySQL()) ? INV_INDEX_CONSTRAINT_MYSQL : INV_INDEX_CONSTRAINT_PSQL, 
                    tableAlias + ((DataSourceFactory.getType().isMySQL()) ? "." : ".ts_vec_") + col.getName(),
                    (DataSourceFactory.getType().isMySQL()) ? DatabaseUtil.prepareForAndBooleanSearch(entry.getKey()) : DatabaseUtil.prepareForAndTsQuerySearch(entry.getKey()))
                        +
                    " OR ";
            }
            constraints = constraints.substring(0, constraints.length() - 4) + ") AND ";
        }
        constraints = constraints.substring(0, constraints.length() - 5);

        // Create the assignment
        return constraints;
    }   
     
}
