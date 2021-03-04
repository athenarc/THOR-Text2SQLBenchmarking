package discoverIR.components;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.database.model.graph.SchemaGraph;

import discoverIR.model.SQLTempTable;
import discoverIR.exceptions.JoinCandidateNotFoundException;
import discoverIR.model.JoinEquation;
import shared.util.Pair;


// The Joiner class is responsible for recognizing Join Patterns
// between SQLTables and creates the JoinEquations representing 
// the join
public class Joiner {        

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
        SQLTable tableConnectedWithLeftTable = leftTable.getConnectedTable();
        SQLTable tableConnectedWithRightTable = rightTable.getConnectedTable();
        
        columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
            tableConnectedWithLeftTable,
            tableConnectedWithRightTable, 
            foreignKeyTable
        );


        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setLeft(
            leftTable.getColumnLikeConnectedTables(                
                columnsInvolvingInJoin.getLeft()
            ) 
        );
        columnsInvolvingInJoin.setRight(
            rightTable.getColumnLikeConnectedTables(                
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
        SQLTable tableConnectedWithLeftTable = leftTable.getConnectedTable();        

        // For all the tables creating the left TempTable try to find a join
        // with the SQLTable rightTable.        
        columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
            tableConnectedWithLeftTable,
            rightTable, 
            foreignKeyTable
        );

        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setLeft(
            leftTable.getColumnLikeConnectedTables(                
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
        SQLTable tableConnectedWithRightTable = rightTable.getConnectedTable();

        // For all the tables creating the Right TempTable try to find a join
        // with the SQLTable rightTable.        
        columnsInvolvingInJoin = schemaGraph.getColumnsJoiningNodes(
            leftTable,
            tableConnectedWithRightTable, 
            foreignKeyTable
        );                

        // Return null if we couldnt Join the tables.
        if (columnsInvolvingInJoin == null) return null;

        // The pair columnsInvolvingInJoin holds columns of the SQLTables 
        // creating the Temp Table. We need to update those columns to 
        // columns contained in the SQLTempTables themselves.
        columnsInvolvingInJoin.setRight(
            rightTable.getColumnLikeConnectedTables(                
                columnsInvolvingInJoin.getRight()
            ) 
        );

        // Return the updated column Pair.
        return columnsInvolvingInJoin;
    }    

}