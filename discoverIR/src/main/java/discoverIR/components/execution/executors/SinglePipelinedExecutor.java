package discoverIR.components.execution.executors;

import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLTable;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.database.model.graph.SchemaGraph;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.util.Pair;

import discoverIR.model.FreeTupleSet;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.TupleSet;
import discoverIR.exceptions.JoinCandidateNotFoundException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


// Input : A list of TupleSets
// Output : A list of OverloadedTuples that are the result of
//          the join of the above TupleSets.
public class SinglePipelinedExecutor extends Executor {

    // A generic SQL SELECT query that returns the tuples contained in the Candidate Network.
    private static final String SQL_SELECT_QUERY =
        "SELECT %s " +  // The columns to select.
        "FROM %s " +  // The tables of the tupleSets.
        "WHERE %s %s " + // The first %s are the JoinEquations and the second the in list constraints.
        "ORDER BY score LIMIT %s";
    protected final static String IN_LIST_ASSIGNMENT = "%s IN (%s)";

    // Public Constructor.
    public SinglePipelinedExecutor(
        SQLDatabase database,
        SchemaGraph schemaGraph,
        Integer maxTuples,
        List<TupleSet> nonFreeTupleSets)
    {
        super(database, schemaGraph, maxTuples, nonFreeTupleSets);
    }

    // Returns a constraint containing specific Primary Key's values in String format.
    // Create a list containing values of the Primary Key (of the TupleSet's Table)
    // which are used to limit the tuples Select query to those tuples that are contain
    // by the tupleSets. This list of values is applied in the WHERE stmt
    // with that format "[columnName] IN ([idsList])".
    protected String getInListConstraintForIds(TupleSet tupleSet, SQLColumn columnInvolvingInJoin) {
        String inListAssignment = new String();

        // Check if the Tuple Set is not a Free tuple Set.
        if (!(tupleSet instanceof FreeTupleSet)) {

            // Get the TupleSets tuples and extract the value of the PK column.
            // This is the id we want
            String idList = new String();
            for (OverloadedTuple tuple: tupleSet.getTuples()) {
                SQLValue value = tuple.getValueOfColumnWithName(columnInvolvingInJoin.getName());
                if (value != null) {
                    idList += value.toString() + ", ";
                }
            }

            // Remove the last ", ".
            if (idList.length() != 0)
                idList = idList.substring(0, idList.length()-2);

            // Create the assignment
            String columnWithAlias = columnInvolvingInJoin.getTableName() + "." + columnInvolvingInJoin.getName();
            inListAssignment = String.format(IN_LIST_ASSIGNMENT, columnWithAlias, idList);
        }

        return inListAssignment;
    }

    // Returns a list of Pairs. Those pairs contain columns where the SQLTable connected with the
    // parameter tupleSet joins with the SQLTables connected with the TupleSets in the candidate List.
    // The columns of the tupleSet parameter are always on the left of the pair. If tupleSet
    // can't join with neither of the candidates throw an Exception.
    private List<Pair<SQLColumn, SQLColumn>> returnJoinColumnsForTupleSet(
        TupleSet tupleSet,
        List<TupleSet> candidates,
        List<Pair<Boolean, Boolean>> referencingTables
     )
     throws JoinCandidateNotFoundException
    {
        // Each tupleSet represents a table. Loop each candidate tupleSet to find
        // the tuple set that joins with.
        List<Pair<SQLColumn, SQLColumn>> joinPairs = new ArrayList<>();
        for (TupleSet joinCandidate: candidates) {
            Pair<SQLColumn, SQLColumn> columnsInvolvingInJoin = null;
            Pair<Boolean, Boolean> referencingTable = new Pair<Boolean, Boolean>(false, false);
            if (joinCandidate.equals(tupleSet)) continue;

            System.out.println("DEBUG" + tupleSet.getTable().getName());
            columnsInvolvingInJoin = this.schemaGraph.getColumnsJoiningNodes(
                tupleSet.getTable(),
                joinCandidate.getTable(),
                referencingTable
            );

            // If we found it then break loop.
            if (columnsInvolvingInJoin != null && !columnsInvolvingInJoin.containsNullObject()) {
                joinPairs.add(columnsInvolvingInJoin);
                referencingTables.add(referencingTable);
            }
        }

        // If no join Candidate was found for this tupleSet throw exception.
        if (joinPairs.isEmpty())
            throw new JoinCandidateNotFoundException(tupleSet);

        // Else return the join list.
        return joinPairs;
    }

    // Creates a string containing the columns for th SELECT part of the SQL query.
    // It inputs a set of columns and get their names, prefixed with the table they
    // belong to and add them to the returned string. The Set of tables is needed
    // to get the score columns. Score columns are separated from the set of columns
    // because they need special handling.
    private String getSelectedColumnsToString(Set<SQLTable> tables, Set<SQLColumn> columns) {
        // The string containing the column names prefixed by the tables aliases.
        String sqlColumnsToProject = new String();
        String sqlScoreColumn = new String();

        // Add the prefixed columns to sqlColumnsToProject string.
        this.columnsToSelect = new ArrayList<>();
        for (SQLColumn column: columns) {
            sqlColumnsToProject += column.getTableName() + "." + column.getName() + ", ";
            this.columnsToSelect.add(column);
        }

        // Create the score column.
        for (SQLTable table: tables) {
            // If the table contains a score column then combine it with the score columns of the other tables.
            SQLColumn column = table.getColumnByName("score");
            if (column != null) {
                sqlScoreColumn += table.getName() + "." + column.getName() + " + ";
            }
        }

        // Add an "score" alias to the sum of the score columns.
        if (!sqlScoreColumn.isEmpty()) {
            // Remove the "+ " and add an " as score" string.
            sqlScoreColumn = sqlScoreColumn.substring(0, sqlScoreColumn.length()-2) + " as score";

            // Add a score column to the columnsToSelect set.
            SQLType columnType = new SQLType("double", 0);
            SQLColumn scoreColumn = new SQLColumn(null, "score", columnType, "");
            this.columnsToSelect.add(scoreColumn);
        } else {
            //remove the last ", "
            sqlColumnsToProject = sqlColumnsToProject.substring(0, sqlColumnsToProject.length() - 2);
        }

        // Add score column to columns to Project.
        sqlColumnsToProject += sqlScoreColumn;

        // Return the String.
        return sqlColumnsToProject;
    }


    // Create the SQL Select Query joining all the TupleSets
    // in the input List and return it in String format.
    private String getSelectQueryToString(List<TupleSet> tupleSetList) throws JoinCandidateNotFoundException {
        // Strings used to fill the parameterized query.
        String sqlTablesList = new String();
        String sqlInListConstraints = new String();
        String sqlListOfJoinEquations = new String();
        Set<SQLTable> tables = new HashSet<>();

        // A set storing unique join equations.
        Set<JoinEquation> equations = new HashSet<>();

        // A set storing column that are going to fill to the select part of the query.
        Set<SQLColumn> columnsToSelect = new HashSet<>();

        // Loop all the tupleSets
        for (TupleSet tupleSet: tupleSetList) {
            // Get the columns that this tuple Set joins with one of the other tuple sets.
            List<Pair<Boolean, Boolean>> referencingTables = new ArrayList<>();
            List<Pair<SQLColumn, SQLColumn>> joinPairs = this.returnJoinColumnsForTupleSet(
                tupleSet,
                tupleSetList,
                referencingTables
            );

            // Add the SQLTable that this tupleSet is connected with, to the sqlTableList string.
            if (!tables.contains(tupleSet.getTable())) {
                sqlTablesList += tupleSet.getTable().getName() + ", ";
                tables.add(tupleSet.getTable());
            }

            // For the number of joins this Tuple get JoinEquations and columns to select
            // for the parameterized query.
            Set<SQLColumn> tupleSetColumnsInvolvingInJoinPairs = new HashSet<>();
            for (int index = 0; index < joinPairs.size(); index++) {
                Pair<SQLColumn, SQLColumn> columnPair = joinPairs.get(index);

                // Create a Join equation.
                JoinEquation joinEquation = new JoinEquation(columnPair.getLeft(), columnPair.getRight());

                // If the tuple Set is not a free tuple set then keep its columns used in the join
                // pairs. We need them to make an In List constraint.
                if (!(tupleSet instanceof FreeTupleSet))
                    tupleSetColumnsInvolvingInJoinPairs.add(columnPair.getLeft()); // TupleSet's columns are always on the left.

                // Add it to the Set.
                equations.add(joinEquation);

                // Add PKs of this join to columns to select.
                if(referencingTables.get(index).getLeft())
                    columnsToSelect.add(columnPair.getRight());
                else
                    columnsToSelect.add(columnPair.getLeft());
            }

            // Create the In list constraint for the columns of the SQLTable connected with this
            // tuple set.
            for (SQLColumn column: tupleSetColumnsInvolvingInJoinPairs) {
                // System.out.println("Column for in list: " + column);
                sqlInListConstraints += this.getInListConstraintForIds(tupleSet, column) + " AND ";
            }

            // Add all the Columns contained from keywords from this TupleSet in the columns set.
            columnsToSelect.addAll(tupleSet.getColumnsContainingKeywords());
        }

        // Add all the Join Equations to the sqlListOfJoinEquations string.
        for (JoinEquation joinEquation: equations) {
            sqlListOfJoinEquations += joinEquation.toString() + " AND ";
        }

        // Create the Select query and return it.
        return String.format(
            SQL_SELECT_QUERY,
            this.getSelectedColumnsToString(tables, columnsToSelect),
            sqlTablesList.substring(0, sqlTablesList.length()-2), // remove the last ", "
            sqlListOfJoinEquations, // Keep the last AND for the next in list segment
            sqlInListConstraints.substring(0, sqlInListConstraints.length()-4) ,
            this.maxTuples.toString()
        );
    }

    // The main function of the executor. Creates an SQL query
    // and contacts the database to get the results.
    public List<OverloadedTuple> execute(List<TupleSet> tupleSetList) throws JoinCandidateNotFoundException {
        // Return in case of empty tupleSetList
        if (tupleSetList.isEmpty()) {
            return null;
        }

        // A Tuple List containing the results.
        List<OverloadedTuple> resultTuples = new ArrayList<OverloadedTuple>();

        // If tupleSetList holds one tupleSet only then return the tuples of this tuple set
        if (tupleSetList.size() == 1) {
            resultTuples.add(tupleSetList.get(0).getTopTuple());
            return resultTuples;
        }

        // Create the SQL SELECT QUERY.
        String selectQuery = this.getSelectQueryToString(tupleSetList);

        // System.out.println("Query :" + selectQuery + "\n");

        // Initialize connection variables.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {
            // Get Connection.
            con = DataSourceFactory.getConnection();

            // Create the statement and execute it.
            stmt = con.createStatement();
            rs = stmt.executeQuery(selectQuery);

            // Get the results
            while (rs.next()) {
                OverloadedTuple tuple = new OverloadedTuple();
                tuple.fill(this.columnsToSelect, rs, this.columnsToSelect.size());
                resultTuples.add(tuple);
            }

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            DatabaseUtil.close(con, stmt, rs);
        }

        // Return the Results
        return resultTuples;
    }

}
