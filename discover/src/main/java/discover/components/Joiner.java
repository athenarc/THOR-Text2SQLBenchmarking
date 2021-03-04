package discover.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import shared.database.model.SQLColumn;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;


import discover.exceptions.JoinCandidateNotFoundException;
import discover.model.JoinableExpression;
import discover.model.JoinablePair;
import discover.model.TupleSet;
import discover.model.execution.JoinEquation;
import discover.model.SQLTempTable;


// The Joiner class is responsible for recognizing Join Patterns
// between SQLTables and creates the JoinEquations representing 
// the join
public class Joiner {

    // Return a list of join equations for joining all the tables in the parameter JoinableExpression.
    // The last 2 parameters if not set to null will be filled by the function with 
    // TupleSets along with their Table's Aliases and with Tables along with their Aliases.
    // But tupleSets and Tables where used by the Joiner Class to create the JoinEquations.
    public static List<JoinEquation> getEquationsForJoinableExpression(
        JoinableExpression joinableExpression,
        SchemaGraph schemaGraph,
        List<Pair<TupleSet, String>> fillTupleSetsUsed,  
        List<Pair<SQLTable, String>> fillTablesUsed) 
        throws JoinCandidateNotFoundException
    {
        List<JoinEquation> joinEquations = new ArrayList<>();  // Stores the result.
        List<Pair<TupleSet, String>> tupleSetsUsedInJoins = new ArrayList<>();
        List<OverloadedTable> tablesUsedInJoins = new ArrayList<>();

        // If the joinableExpression is a tupleSet then there is no JoinEquation to return.
        if (joinableExpression instanceof TupleSet) {
            // Insert the one Table and TupleSet in both lists with the same alias.
            tablesUsedInJoins.add(
                new OverloadedTable(((TupleSet) joinableExpression).getTable(), SQLQueries.ALIAS_LETTER + "0", 0)
            );         
            tupleSetsUsedInJoins.add(
                new Pair<>((TupleSet) joinableExpression, SQLQueries.ALIAS_LETTER + "0")
            );  
            
        }
        // Else find a way to join the Tables inside the JoinablePair.
        else {
            JoinablePair pair = (JoinablePair) joinableExpression;         
            tablesUsedInJoins = recJoiner(
                pair, joinEquations, new IntegerWrapper(0), schemaGraph, tupleSetsUsedInJoins
            );
        }

        // Fill the two parameters
        Joiner.fillTablesWithAssignments(fillTablesUsed, tablesUsedInJoins);
        Joiner.fillTupleSets(fillTupleSetsUsed, tupleSetsUsedInJoins);


        // Return the Join Equations
        return joinEquations;
    }

    // Fills a list of tables along with their aliases.
    // Those tables where used by the joiner to achieve joins.    
    public static void fillTablesWithAssignments(
        List<Pair<SQLTable,String>> tablesToFill, 
        List<OverloadedTable> tablesUsedInJoins) 
    {        
        if (tablesToFill == null) return;

        // Loop the tablesUsedInJoins and create the list of Pairs.
        for (OverloadedTable table: tablesUsedInJoins) {
            tablesToFill.add(new Pair<>(table.table, table.alias));
        }
    }

    // Fills a list of tupleSets along with their Table's aliases.
    // Those tupleSets where used by the joiner to achieve joins.    
    public static void fillTupleSets(
        List<Pair<TupleSet, String>> tupleSetsToFill, 
        List<Pair<TupleSet, String>> tupleSetsUsedInJoins) 
    {        
        if (tupleSetsToFill == null) return;
        tupleSetsToFill.addAll(tupleSetsUsedInJoins);        
    }


    // Get a list of OverloadedTable from a JoinableExpression.
    // - If the expre
    private static List<OverloadedTable> getTableOrCallRecJoin(
        JoinableExpression joinableExpression,        
        List<JoinEquation> joinEquations,
        IntegerWrapper aliasesCounter,
        SchemaGraph schemaGraph,
        List<Pair<TupleSet, String>> tupleSetsUsedInJoins)
        throws JoinCandidateNotFoundException
    {
        // The resulting tables.
        List<OverloadedTable> overladedTables = null;

        // Get the the table from the JoinableExpression.
        SQLTable table = joinableExpression.getTable();
        if (table != null) {
            overladedTables = new ArrayList<>();
            // Create The table Alias and add it in the table list.
            String tableAlias = SQLQueries.ALIAS_LETTER + aliasesCounter.toString();
            overladedTables.add( new OverloadedTable(table, tableAlias, 0) );
            // Increment the alias counter
            aliasesCounter.incrementInteger();

            // Add the expression in the tupleSet array if it is one.
            if (joinableExpression instanceof TupleSet)
                tupleSetsUsedInJoins.add(new Pair<>((TupleSet) joinableExpression, tableAlias));
        }
        // If there is no Table stored in the JoinableExpression then the expression is 
        // a joinable pair, which was not assigned as an intermediate Result. Call the recJoiner
        // to get the wanted Tables.
        else if (joinableExpression instanceof JoinablePair) {                            
            overladedTables = recJoiner(
                (JoinablePair) joinableExpression, joinEquations, 
                aliasesCounter, schemaGraph, tupleSetsUsedInJoins
            );
        }

        // Return the tables.
        return overladedTables;
    }


    // Recursively find the tables contained in a JoinablePair and join them creating a
    // JoinEquation for each pair.
    private static List<OverloadedTable> recJoiner(
        JoinablePair pair,
        List<JoinEquation> joinEquations,
        IntegerWrapper aliasesCounter,
        SchemaGraph schemaGraph,
        List<Pair<TupleSet, String>> tupleSetsUsedInJoins)
        throws JoinCandidateNotFoundException        
    {

        // Get possibleCandidateTables from left and right joinableExpressions of the pair
        List<OverloadedTable> leftPossibleTables = Joiner.getTableOrCallRecJoin(
            pair.getLeft(), joinEquations, aliasesCounter, schemaGraph, tupleSetsUsedInJoins
        );
        List<OverloadedTable> rightPossibleTables =  Joiner.getTableOrCallRecJoin(
            pair.getRight(), joinEquations, aliasesCounter, schemaGraph, tupleSetsUsedInJoins
        );

        // Order Left and Right Overloaded Tables with acceding order of their 
        // timesUsedToJoin field. This way we are more likely to pick the best joins.
        Collections.sort(leftPossibleTables, new OverloadedTable.JoinFrequencyComparator());
        Collections.sort(rightPossibleTables, new OverloadedTable.JoinFrequencyComparator());

        // Get one Join Equation out of those tables.
        JoinEquation joinEquation = null;
        for (OverloadedTable leftTable: leftPossibleTables) {
            for (OverloadedTable rightTable: rightPossibleTables) {
                // Get the joinEquation
                joinEquation = Joiner.joinTwoTables(leftTable, rightTable, schemaGraph);
                if (joinEquation != null) {
                    // Store the joinEquation.
                    joinEquations.add(joinEquation);

                    // Increment the Table's timesUsedToJoin.
                    leftTable.timesUsedToJoin++;
                    rightTable.timesUsedToJoin++;

                    // Break the inner Loop.
                    break;
                }
            }

            // Break the outer Loop.
            if (joinEquation != null)           
                break;
        }
        
        // If we couldn't get a joinEquation then throw an exception
        if (joinEquation == null) throw new JoinCandidateNotFoundException(pair);

        // Concatenate both lists of tables and return them as possible 
        // candidate Tables for the pair created by this pair.
        leftPossibleTables.addAll(rightPossibleTables);
        return leftPossibleTables;
    }


    // Create a joinEquation for the left and right Table parameters.
    // If no join could be found return null.
    private static JoinEquation joinTwoTables(
        OverloadedTable leftTable,
        OverloadedTable rightTable, 
        SchemaGraph schemaGraph)        
    {
        JoinEquation joinEquation = null; // The joinEquation.                            

        // Use the schemaGraph to get the pair of columns on which those two tables join.
        Pair<Boolean, Boolean> foreignKeyTable = new Pair<Boolean,Boolean>(false, false);
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = Joiner.checkJoinForTables(
            leftTable.table, rightTable.table,
            foreignKeyTable, schemaGraph
        );

        
        // If the tables dont join continue else create the JoinEquation and break the loop.
        if (columnsInvolvingInJoin != null) {
            // Create the joinEquation.
            if (foreignKeyTable.getRight())
                joinEquation = new JoinEquation(
                    columnsInvolvingInJoin.getLeft(), leftTable.alias,
                    columnsInvolvingInJoin.getRight(), rightTable.alias
                );
            else
                joinEquation = new JoinEquation(
                    columnsInvolvingInJoin.getRight(), rightTable.alias,
                    columnsInvolvingInJoin.getLeft(), leftTable.alias
                );                        
        }            

        // Return the JoinEquation.
        return joinEquation;
    }

    // Create a joinEquation for the left and right Table parameters.
    // If no join could be found return null.
    public static JoinEquation getJoinEquation(
        Pair<SQLTable,String> leftTable,
        Pair<SQLTable,String> rightTable, 
        SchemaGraph schemaGraph)
        throws JoinCandidateNotFoundException        
    {
        JoinEquation joinEquation = null; // The joinEquation.                            

        // Use the schemaGraph to get the pair of columns on which those two tables join.
        Pair<Boolean, Boolean> foreignKeyTable = new Pair<Boolean,Boolean>(false, false);
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = Joiner.checkJoinForTables(
            leftTable.getLeft(), rightTable.getLeft(),
            foreignKeyTable, schemaGraph
        );

        // If the tables dont join continue else create the JoinEquation and break the loop.
        if (columnsInvolvingInJoin != null) {
            // Create the joinEquation.
            if (foreignKeyTable.getRight())
                joinEquation = new JoinEquation(
                    columnsInvolvingInJoin.getLeft(), leftTable.getRight(),
                    columnsInvolvingInJoin.getRight(), rightTable.getRight()
                );
            else
                joinEquation = new JoinEquation(
                    columnsInvolvingInJoin.getRight(), rightTable.getRight(),
                    columnsInvolvingInJoin.getLeft(), leftTable.getRight()
                );                        
        }
        else 
            throw new JoinCandidateNotFoundException(leftTable.getLeft(), rightTable.getLeft());

        // Return the JoinEquation.
        return joinEquation;
    }


    // Returns a pair with columns involving in join for the leftTable and rightTable.
    // If we cant join them returns null.
    private static Pair<SQLColumn, SQLColumn> checkJoinForTables(
        SQLTable leftTable, 
        SQLTable rightTable,
        Pair<Boolean, Boolean> foreignKeyTable,
        SchemaGraph schemaGraph)
    {
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = null;        
        SQLTempTable tempLeftTable = null;
        SQLTempTable tempRightTable = null;

        // Check if the two tables are TempTables.
        if (leftTable instanceof SQLTempTable)
            tempLeftTable = (SQLTempTable) leftTable;
        if (rightTable instanceof SQLTempTable)
            tempRightTable = (SQLTempTable) rightTable;
        
        // Call the right function to check how those tables Join.
        if (tempLeftTable != null && tempRightTable != null) {
            columnsInvolvingInJoin = 
                getColumnsInvolvingInJoin(tempLeftTable, tempRightTable, foreignKeyTable, schemaGraph);
        }
        else if (tempLeftTable != null) {
            columnsInvolvingInJoin = 
                getColumnsInvolvingInJoin(tempLeftTable, rightTable, foreignKeyTable, schemaGraph);
        }
        else if (tempRightTable != null) {
            columnsInvolvingInJoin = 
                getColumnsInvolvingInJoin(leftTable, tempRightTable, foreignKeyTable, schemaGraph);
        }
        else {
            // If we dont have temp Tables Just Check The SchemaGraph.
            columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
                leftTable,
                rightTable, 
                foreignKeyTable
            );
        }

        return columnsInvolvingInJoin;
    }
    

    // Get Columns Joining Two tempTables
    private static Pair<SQLColumn, SQLColumn> getColumnsInvolvingInJoin(
        SQLTempTable leftTable, 
        SQLTempTable rightTable,
        Pair<Boolean, Boolean> foreignKeyTable,
        SchemaGraph schemaGraph)
    {   
        // The Columns Involving in Join.
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = null;

        // Get the tables contained in the left TempTable and right TempTable.
        List<SQLTable> tablesCreatingLeftTable = leftTable.getContainedTables();
        List<SQLTable> tablesCreatingRightTable = rightTable.getContainedTables();

        // For all the tables creating the left TempTable try to find a join
        // with any of the tables creating the right TempTable
        for (SQLTable tableCreatingLeftTable: tablesCreatingLeftTable) {
            for (SQLTable tableCreatingRightTable: tablesCreatingRightTable) {
                columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
                    tableCreatingLeftTable,
                    tableCreatingRightTable, 
                    foreignKeyTable
                );

                // If we found a way to join the tempTables break.
                if (columnsInvolvingInJoin != null) break;
            }
            // If we found a way to join the tempTables break.
            if (columnsInvolvingInJoin != null) break;        
        }

        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setLeft(
            leftTable.getColumnFromContainedTableByName(
                columnsInvolvingInJoin.getLeft().getTable(),
                columnsInvolvingInJoin.getLeft()
            ) 
        );
        columnsInvolvingInJoin.setRight(
            rightTable.getColumnFromContainedTableByName(
                columnsInvolvingInJoin.getRight().getTable(),
                columnsInvolvingInJoin.getRight()
            ) 
        );

        // Return the updated column Pair.
        return columnsInvolvingInJoin;
    }

    // Get Columns Joining an SQLTempTable and an SQLTable.
    private static Pair<SQLColumn, SQLColumn> getColumnsInvolvingInJoin(
        SQLTempTable leftTable, 
        SQLTable rightTable,
        Pair<Boolean, Boolean> foreignKeyTable,
        SchemaGraph schemaGraph)
    {   
        // The Columns Involving in Join.
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = null;
        // Get the tables contained in the left TempTable.
        List<SQLTable> tablesCreatingLeftTable = leftTable.getContainedTables();        

        // For all the tables creating the left TempTable try to find a join
        // with the SQLTable rightTable.
        for (SQLTable tableCreatingLeftTable: tablesCreatingLeftTable) {        
            columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
                tableCreatingLeftTable,
                rightTable, 
                foreignKeyTable
            );

            // If we found a way to join the tempTables break.
            if (columnsInvolvingInJoin != null) break;        
        }

        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setLeft(
            leftTable.getColumnFromContainedTableByName(
                columnsInvolvingInJoin.getLeft().getTable(),
                columnsInvolvingInJoin.getLeft()
            ) 
        );

        // Return the updated column Pair.
        return columnsInvolvingInJoin;
    }


    // Get Columns Joining for an SQLTable and an SQLTempTable.
    private static Pair<SQLColumn, SQLColumn> getColumnsInvolvingInJoin(
        SQLTable leftTable, 
        SQLTempTable rightTable,
        Pair<Boolean, Boolean> foreignKeyTable,
        SchemaGraph schemaGraph)
    {   
        // The Columns Involving in Join.
        Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = null;

        // Get the tables contained in the right TempTable.        
        List<SQLTable> tablesCreatingRightTable = rightTable.getContainedTables();

        // For all the tables creating the Right TempTable try to find a join
        // with the SQLTable rightTable.
        for (SQLTable tableCreatingRightTable: tablesCreatingRightTable) {
            columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
                leftTable,
                tableCreatingRightTable, 
                foreignKeyTable
            );

            // If we found a way to join the tempTables break.
            if (columnsInvolvingInJoin != null) break;
        }        

        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setRight(
            rightTable.getColumnFromContainedTableByName(
                columnsInvolvingInJoin.getRight().getTable(),
                columnsInvolvingInJoin.getRight()
            ) 
        );

        // Return the updated column Pair.
        return columnsInvolvingInJoin;
    }    

}



// The OverloadedTable holds some extra information about an SQLTable
// that are useful for achieving joins.
class OverloadedTable {

    // Compares in an acceding fashion the frequency of the Tables used in a join.
    static class JoinFrequencyComparator implements Comparator<OverloadedTable> {
        @Override
        public int compare(OverloadedTable arg0, OverloadedTable arg1) {
            return arg0.timesUsedToJoin.compareTo(arg1.timesUsedToJoin);
        }
    }


    SQLTable table;
    String alias;
    Integer timesUsedToJoin;

    // Public Constructor.
    OverloadedTable(SQLTable table, String alias, Integer timesUsedToJoin) {
        this.table = table; this.alias = alias; this.timesUsedToJoin = timesUsedToJoin;
    }
}

// Wraps an Integer to assure the "pass by reference" on the recursive functions.
class IntegerWrapper {
    Integer integer;

    // Public constructor.
    IntegerWrapper(Integer integer) { this.integer = integer; }
    
    // Increments the integer by one.
    void incrementInteger() { this.integer++; }

    @Override
    public String toString() {
        return this.integer.toString();
    }
}