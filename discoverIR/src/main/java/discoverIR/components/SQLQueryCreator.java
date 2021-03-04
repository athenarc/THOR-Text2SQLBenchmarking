package discoverIR.components;

import java.util.ArrayList;
import java.util.List;

import discoverIR.model.JoinEquation;
import discoverIR.model.JoinableFormat;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.SQLTempTable;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLColumn;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.SQLValue;
import shared.database.model.SQLQuery;
import shared.util.Pair;
import shared.util.PrintingUtils;

public class SQLQueryCreator {


    // Creates an SQL InsertIntoSelect Query for the SQLTempTable tempTable with the help of 
    // the JoinableFormat joinableFormat used to create the SQLTempTable.
    public static String createSQLInsertIntoSelectQuery(JoinableFormat joinableFormat, SQLTempTable tempTable) {
        // Simply add the TempTables name and a selectQuery for the joinableFormat
        return String.format(
            SQLQueries.SQL_INSERT_INTO_SELECT_QUERY,
            tempTable.getName(),
            SQLQueryCreator.createSQLSelectQuery(joinableFormat).toSelectQuery()
        );
    }
    

    // Creates an SQLInsertInto Query for the SQLTempTable tempTable. The values inserted is 
    // a list of OverloadedTuples.
    public static String createSQLInsertIntoValuesQuery(List<OverloadedTuple> tuples, SQLTempTable tempTable) {        
        String tableWithColumns = tempTable.getName();
        
        // Add the columns in the above String (ordered as inside the Tuples)
        tableWithColumns += " (";
        for (SQLColumn column: tuples.get(0).getAttributes())
            tableWithColumns += column.getName() + ", ";
        tableWithColumns += "score )"; 

        return String.format(
            SQLQueries.SQL_INSERT_INTO_QUERY,
            tableWithColumns,
            SQLQueryCreator.getTupleListToString(tuples)
        );                        
    }

    // Creates an SQL Select Query for the JoinableFormat joinableFormat.
    public static SQLQuery createSQLSelectQuery(JoinableFormat joinableFormat) {
        SQLQuery query = new SQLQuery();
                
        // First create the SELECT part of the query, projecting the columns. {        
        for(Pair<SQLColumn,String> pair: joinableFormat.getColumnsFromTables()) {
            query.getSelectElements().add( pair.getRight() + "." + pair.getLeft().getName());
        }        

        // Handle the special case where there are score columns.
        String scoreColumnsSum = new String();
        for(Pair<SQLColumn,String> pair: joinableFormat.getScoreColumns()) {
            scoreColumnsSum +=  pair.getRight() + "." + pair.getLeft().getName() + " + ";
        }

        // Add a "score" alias to the sum of the score columns and a score column to columnsToSelect.
        if (!scoreColumnsSum.isEmpty()) {
            scoreColumnsSum = scoreColumnsSum.substring(0, scoreColumnsSum.length() - 2) + " as score";
            query.getSelectElements().add( scoreColumnsSum );
        }
        // }
        
        // Then create the FROM part of the query, the tables with their aliases. {        
        for(Pair<SQLTable, String> pair: joinableFormat.getTablesToJoin()) {
            // query.getFromElements().add( pair.getLeft().getName() + SQLQueries.ALIAS_SPECIFIER + pair.getRight());
            query.addTableWithAlias(pair.getLeft().getName(), pair.getRight());
        }         
        // }
        
        // Create the WHERE part of the query. The where part has 2 subParts :
        //  - The JoinEquations responsible for joining the above tables. {             
        for(JoinEquation joinEquation: joinableFormat.getJoinEquations()) {
            query.getJoinEquations().add( joinEquation.toString() );
        }        
        // }

        //  - The IN LIST Constraints if there are any. {        
        for(String inListConstraint: joinableFormat.getInListConstraints()) {
            query.getInListConstraints().add( inListConstraint );
        }           
        // }

        return query;
    }


    // Return the CREATE TABLE query for the tempTable in string format.
    public static String createSQLCreateTableQuery(SQLTempTable tempTable) {
        // First create the Columns and Types subString.
        String columnsAndTypes = new String();
        for (SQLColumn column: tempTable.getColumns()) {
            columnsAndTypes += "\t" + String.format(SQLQueries.COLUMN_TYPE_PAIR, column.getName(), column.getType()) + ",\n";
        }
        columnsAndTypes = columnsAndTypes.substring(0, columnsAndTypes.length()-2) + "\n";

        // Then Create the Primary Key Assignment.
        String primaryKey = new String();
        for (SQLColumn column: tempTable.getPrimaryKey()) {
            primaryKey += column.getName() + ", ";
        }

        
        String primaryKeyAssignment = new String(); 
        if (!primaryKey.isEmpty()) 
            primaryKeyAssignment = String.format(
                SQLQueries.PRIMARY_KEY_CONSTRAINT, 
                primaryKey.substring(0, primaryKey.length()-2)
            ) + "\n";                

        return String.format(
            SQLQueries.SQL_CREATE_TABLE_QUERY, tempTable.getName(), columnsAndTypes, 
            ((!primaryKeyAssignment.isEmpty()) ? ("," + primaryKeyAssignment) : (" "))
        );
    }
    
    
    // Creates a list with the Tuple's Values comma separated,
    // to use it in the INSERT INTO SQL query.
    private static String getTupleListToString(List<OverloadedTuple> tupleList) {
        String tupleValuesToStr = new String();        

        // Loop all the tuples.
        for (OverloadedTuple tuple: tupleList) {            
            tupleValuesToStr += "(";
            for (SQLValue value: tuple.getValues()) {
                // Depending on Value's Type escape special char "'" and add quotes around them.
                if (value.getType().isArithmetic())
                    tupleValuesToStr += value.toString();            
                else                    
                    tupleValuesToStr += "\'" + DatabaseUtil.escapeStrValue(value.toString())  + "\'";
                
                tupleValuesToStr += ", ";
            }

            // Add score
            tupleValuesToStr += tuple.getScore() + "), ";             
        }
        // Remove the Last ", ".
        tupleValuesToStr = tupleValuesToStr.substring(0, tupleValuesToStr.length() - 2);

        return tupleValuesToStr;
    }

}