package discover.components;

import java.util.List;

import shared.database.model.SQLColumn;
import shared.database.model.SQLQueries;
import shared.database.model.SQLQuery;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import shared.database.model.SQLValue;
import shared.util.Pair;
import shared.util.PrintingUtils;


import discover.model.execution.JoinEquation;
import discover.model.execution.JoinableFormat;
import discover.model.SQLTempTable;


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
    // a list of SQLTuples.
    public static String createSQLInsertIntoValuesQuery(List<SQLTuple> tuples, SQLTempTable tempTable) {
        return String.format(
            SQLQueries.SQL_INSERT_INTO_QUERY,
            tempTable.getName(),
            SQLQueryCreator.getTupleListToString(tuples)
        );                        
    }


    /**
     * Create an sql view for the TempTable using the joinable format.
     * 
     * @param joinableFormat
     * @param tempTable
     * @return
     */
    public static String createViewQuery(JoinableFormat joinableFormat, SQLTempTable tempTable) {        
        SQLQuery query = new SQLQuery();  // The query object to fill.
        
        // First create the SELECT part of the query, projecting the columns. {
        for(Pair<SQLColumn,String> pair: joinableFormat.getColumnsFromTables()) {
            // For each column use an alias so it matches with the TempTable's columns.            
            SQLColumn viewsColumn = tempTable.getColumnFromContainedTableByName(pair.getLeft().getTable(), pair.getLeft());
            query.getSelectElements().add( pair.getRight() + "." + pair.getLeft().getName() + " AS " + viewsColumn.getName());
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


        //  - The Value Constraints if there are any. {        
        for(String valueConstraint: joinableFormat.getValueConstraints()) {
            query.getValueConstraints().add( valueConstraint );
        }           
        // }
        

        return String.format(
                SQLQueries.SQL_CREATE_VIEW_QUERY, 
                tempTable.getName(),
                query.toSelectQuery()
            );
    }

    

    // Creates an SQL Select Query for the JoinableFormat joinableFormat.
    public static SQLQuery createSQLSelectQuery(JoinableFormat joinableFormat) {
        SQLQuery query = new SQLQuery();
        
        // First create the SELECT part of the query, projecting the columns. {
        for(Pair<SQLColumn,String> pair: joinableFormat.getColumnsFromTables()) {
            query.getSelectElements().add( pair.getRight() + "." + pair.getLeft().getName() );
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


        //  - The Value Constraints if there are any. {        
        for(String valueConstraint: joinableFormat.getValueConstraints()) {
            query.getValueConstraints().add( valueConstraint );
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
        if (primaryKey.isEmpty()) {
            primaryKeyAssignment = String.format(
                SQLQueries.PRIMARY_KEY_CONSTRAINT, 
                primaryKey.substring(0, primaryKey.length()-2)
            ) + "\n";
        }  

        return String.format(
            SQLQueries.SQL_CREATE_TABLE_QUERY, tempTable.getName(), columnsAndTypes, 
            ((!primaryKeyAssignment.isEmpty()) ? ("," + primaryKeyAssignment) : (" "))
        );
    }
    
    
    // Creates a list with the Tuple's Values comma separated,
    // to use it in the INSERT INTO SQL query.
    private static String getTupleListToString(List<SQLTuple> tupleList) {
        String tupleValuesToStr = new String();

        // Loop all the tuples.
        for (SQLTuple tuple: tupleList) {
            tupleValuesToStr += "(";
            for (SQLValue value: tuple.getValues()) {
                // Depending on Value's Type escape special char "'" and add quotes around them.
                if (value.getType().isArithmetic())
                    tupleValuesToStr += value.toString();            
                else                    
                    tupleValuesToStr += "\'" + PrintingUtils.escapeCharacter(value.toString(), '\'', '\'') + "\'";
                
                tupleValuesToStr += ", ";
            }

            // Remove the Last ", " and add "), ".
            tupleValuesToStr = tupleValuesToStr.substring(0, tupleValuesToStr.length() - 2) + "), ";
        }
        // Remove the Last ", ".
        tupleValuesToStr = tupleValuesToStr.substring(0, tupleValuesToStr.length() - 2);

        return tupleValuesToStr;
    }

}