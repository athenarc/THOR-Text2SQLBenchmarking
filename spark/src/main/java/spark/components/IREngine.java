package spark.components;

import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLTable;
import shared.connectivity.thor.response.Table;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;

import spark.model.OverloadedTuple;
import spark.model.TupleSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.HashSet;

// The IREngine component which queries the database single-attribute indexes
// to create a tuple set for every table (relation).
// Input: Query Q and a database.
// Output: List of tuple sets for each relation.
//
// Given a query Q and a table (relation) Ri the tuple set of Ri
// is the set of all tuples of Ri for which the RDBMS returned a non-zero score.
public class IREngine {

    // This query is executed against all indexes of the database
    // and returns all tuples of a relation in which keywords of the query were found.
    // The tuples are returned sorted in descending order by their score.
    private final static String INV_INDEX_QUERY =
            "SELECT *, MATCH (%s) AGAINST (? IN NATURAL LANGUAGE MODE) AS score " +
            "FROM %s WHERE MATCH (%s) AGAINST (? IN NATURAL LANGUAGE MODE);";

    private List<String> keywords; // The keywords of the input query.
    private List<TupleSet> tupleSets; // Contains the tuple set of every table in the database.
    private SQLDatabase database;

    // Stats
    private String query;
    private Integer tablesContainingQuery;
    private Integer rowsMatchingQuery;
    private Integer rowsOfIoSql;


    public IREngine(List<String> keywords, SQLDatabase database) {
        this.keywords = keywords;
        this.tupleSets = new ArrayList<TupleSet>();
        this.database = database;
    }    

    // Getters and Setters.
    public List<String> getKeywords() {
        return this.keywords;
    }

    public List<TupleSet> getTupleSets() {
        return this.tupleSets;
    }

    public SQLDatabase getDatabase() {
        return this.database;
    }

    // Returns a list (of TupleSet objects) that associates every table in the database with its non-free tuple set.
    public List<TupleSet> generateTupleSets(String query) {
        this.tablesContainingQuery = 0;
        this.rowsMatchingQuery = 0;
        this.rowsOfIoSql = 0;
        this.query = query;

        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();

            // Generate the tuple set of every table in the database.
            for (SQLTable table : this.database.getTables()) {
                TupleSet tupleSet = generateTupleSetOfTable(query, table, con, stmt, rs);
                rowsOfIoSql += tupleSet.getSize();
                
                // Only save the non-free (non-empty) tuple sets.
                if (!tupleSet.isEmpty()) {
                    this.tupleSets.add(tupleSet);
                    tablesContainingQuery++;
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection.
            DatabaseUtil.close(con, stmt, rs);
        }

        return this.tupleSets;
    }

    // Returns the tuple set of a given table.
    private TupleSet generateTupleSetOfTable(String query, SQLTable table,
                Connection con, PreparedStatement stmt, ResultSet rs) throws SQLException {
        Set<OverloadedTuple> tuples = new HashSet<OverloadedTuple>();
        Set<SQLColumn> columnsContainingKeywords = new HashSet<SQLColumn>();

        // Execute the INV_INDEX_QUERY against every column's index.
        for (SQLColumn column : table.getColumns()) {
            // FULLTEXT indexes can only be built on columns with strings as values.
            if (column.getType().isTextual() && column.isIndexed()) {
                // Prepare the query to execute.
                String indexQuery = String.format(INV_INDEX_QUERY, column.getName(), table.getName(), column.getName());
                stmt = con.prepareStatement(indexQuery);
                stmt.setString(1, query);
                stmt.setString(2, query);

                // Execute the query and store the tuples.
                rs = stmt.executeQuery();
                while (rs.next()) {
                    // Create an OverloadedTuple object and save it.
                    OverloadedTuple tuple = new OverloadedTuple();
                    tuple.fill(table.getColumns(), rs);
                    tuples.add(tuple);
                    columnsContainingKeywords.add(column);
                }
            }
        }

        // Create the tuple set of the table.
        // The tuples are stored in a list sorted in descending order based on their score.
        TupleSet tupleSet = new TupleSet(table, columnsContainingKeywords, tuples);
        tupleSet.computeKeywordStatistics(this.keywords);

        // Store the rows matched the query.
        rowsMatchingQuery += tupleSet.getSize();

        // Finds and saves the keywords of the query that the tuple set contains.
        tupleSet.setKeywords(this.keywords);

        return tupleSet;
    }

    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {        
        List<String> columnTitles = new ArrayList<>();   // The column titles.
        List<Table.Row> rows = new ArrayList<>();        // The table rows.
        
        columnTitles.addAll(Arrays.asList("Query", "Tables containing query", "Rows matching query"));
                        
        rows.add(new Table.Row( Arrays.asList(
            this.query , this.tablesContainingQuery.toString(),
            this.rowsMatchingQuery.toString()))
        );
                
        // Return the table containing the Components Info.
        return new Table("Mappings", columnTitles, rows);
    }

    /**
     * @return the tablesContainingQuery
     */
    public Integer getTablesContainingQuery() {
        return this.tablesContainingQuery;
    }


    public Integer getRowsOfIoSql() {
        return rowsOfIoSql;
    }
}
