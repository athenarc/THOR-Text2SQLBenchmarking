package discover.components;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import discover.model.TupleSet;
import shared.connectivity.thor.response.Table;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import shared.util.Pair;
import shared.util.Timer;

// Input: Set of m keywords {k1, ..., km} and a database.
// Output: Sets of basic tuples for each relation and keyword.
//
// For a keyword k and a relation Ri the corresponding set of basic tuples
// holds all the tuples of Ri which contain the keyword k.
public class MasterIndex {

    // The keywords of the input query.    
    private List<String> keywords; 

    // The database.
    private SQLDatabase database;

    // Statistics
    private long totalSizeOfSqlIO = 0;  // The bytes exchanges with SQL sever
    private long totalRowsOfSqlIO = 0;  // The rows returned from InvertedIndex searches
    private double timeGeneratingSets;
    private HashMap<String, Pair<Integer, Integer>> keywordsToNumOfTuples;  // Stores how many tuples where found that contain that keywords value
    private Integer numberOfRelations;  // Stores the number of tables where all the keywords where found.

    public MasterIndex(List<String> keywords, SQLDatabase database) {
        this.keywords = keywords;        
        this.database = database;
        this.numberOfRelations = 0;
        this.keywordsToNumOfTuples = new HashMap<>();
    }


    // Getters and Setters.
    public List<String> getKeywords() { return this.keywords; }
    public SQLDatabase  getDatabase() { return this.database; }
    
    public void setDatabase(SQLDatabase database)  { this.database = database; }    
    public void setKeywords(List<String> keywords) { this.keywords = keywords; }


    // Generates all basic tuple sets. One for every keyword and every table.
    public List<TupleSet> generateBasicTupleSets() {        

        // Start the timer.
        Timer timer = new Timer();
        timer.start(); 

        // Initialize variables.
        List<TupleSet> basicTupleSets = new ArrayList<TupleSet>(); 
        Set<String> tables = new HashSet<>();       
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();
            totalSizeOfSqlIO = 0;
            totalRowsOfSqlIO = 0;

            for (String keyword : this.keywords) {
                Integer tuplesNumber = 0;
                Integer relationsNumber = 0;
                for (SQLTable table : this.database.getTables()) {
                    // Get the tuples of the current table that contain the current keyword.
                    Set<SQLColumn> columnsContainingKeywords = new HashSet<>();
                    Set<SQLTuple> tuples = getTuplesContainingKeyword(keyword, table, con, stmt, rs, columnsContainingKeywords);
                    totalRowsOfSqlIO += tuples.size();
                    
                    // Store the stats
                    if (!tuples.isEmpty()) {
                        tuplesNumber += tuples.size();
                        relationsNumber++;
                        tables.add(table.getName());
                    }                

                    // Create a basic tuple set for the keyword and the table.
                    if (!tuples.isEmpty()) {
                        TupleSet ts = new TupleSet(table, keyword, columnsContainingKeywords, tuples);
                        ts.getKeywords2columns().put(keyword, columnsContainingKeywords);
                        basicTupleSets.add(ts);
                    }
                }

                // Update the statistics
                this.keywordsToNumOfTuples.put(keyword, new Pair<>(tuplesNumber, relationsNumber));
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection.
            DatabaseUtil.close(con, stmt, rs);
        }

        // Stop the timer.
        this.timeGeneratingSets = timer.stop();
        this.numberOfRelations = tables.size();        

        // Return the basicTupleSets.
        return basicTupleSets;
    }

    // Returns the tuples of a table in which a given keyword was found.
    public Set<SQLTuple> getTuplesContainingKeyword(
        String keyword, SQLTable table,
        Connection con, PreparedStatement stmt,
        ResultSet rs, Set<SQLColumn> columnsContainingKeywords) throws SQLException     
    {
        Set<SQLTuple> tuples = new HashSet<SQLTuple>();        

        // Execute the query against each column's index for the keyword.
        for (SQLColumn column : table.getColumns()) {
            // FULLTEXT indexes can only be built on columns with strings as values.
            if (column.getType().isTextual() && column.isIndexed()) {

                // Prepare the query to execute.
                // String query = String.format(SQLQueries.INV_INDEX_QUERY, "*", table.getName(), column.getName());
                String query = this.database.getQuery()
                    .select("*")
                    .from(table.getName())
                    .where().addInvIndexCond(column).endWhere()
                    .toSQL();

                stmt = con.prepareStatement(query);
                stmt.setString(1, this.database.prepareForAndFullTextSearch(keyword));                

                // Execute the query and store the tuples.
                rs = stmt.executeQuery();
                // totalSizeOfSqlIO += InstrumentationAgent.getObjectSize(rs);

                while (rs.next()) {
                    SQLTuple tuple = new SQLTuple();
                    tuple.fill(this.database, rs);                    

                    // Add the tuple to the tuple set of the current keyword and relation.
                    tuples.add(tuple);

                    // Add the column which contains this keyword.
                    columnsContainingKeywords.add(column);
                }
            }
        }

        // Return the tuples.
        return tuples;
    }

    // Print the statistics
    public void printStats() {
        System.out.println("MASTER INDEX STATS :");
        System.out.println("\tTime to generate BasicTupleSets: " + this.timeGeneratingSets);
    }

    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {        
        String tableTittle = "Mappings";                 // The table title.
        List<String> columnTitles = new ArrayList<>();   // The column titles.
        List<Table.Row> rows = new ArrayList<>();        // The table rows.

        // Each row will contain 3 values, the keyword and the number of Tables and the number of Rows
        columnTitles.addAll(Arrays.asList("Keyword", "Tables containing Keyword", "Rows matching Keyword"));
        
        for (Map.Entry<String, Pair<Integer, Integer>> entry: this.keywordsToNumOfTuples.entrySet()) {
            rows.add(new Table.Row( Arrays.asList(
                entry.getKey(), entry.getValue().getRight().toString(),
                entry.getValue().getLeft().toString() ))
            );
        }
                
        // Return the table containing the Components Info.
        return new Table(tableTittle, columnTitles, rows);
    }

    /**
     * @return The number or Relations where all the keywords of the query were found.
     */
    public Integer getRelationsFromMappings() {
        return this.numberOfRelations;
    }


    public HashMap<String,Integer> getKeywordsToNumberOfTuples() {
        HashMap<String,Integer> hm = new HashMap<>();
        for (Map.Entry<String, Pair<Integer, Integer>> entry: this.keywordsToNumOfTuples.entrySet()) {
            hm.put(entry.getKey(), entry.getValue().getRight()); // Here get the relations
        }

        return hm;
    }

    public long getTotalRowsOfSqlIO() {
        return totalRowsOfSqlIO;
    }
}
