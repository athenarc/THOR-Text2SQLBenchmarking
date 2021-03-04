package shared.benchmark;

import java.sql.Statement;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Map.Entry;

import com.google.protobuf.MapEntry;

import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseIndexManager;
import shared.database.connectivity.DatabaseUtil;
import shared.database.model.DatabaseType;
import shared.database.model.Query;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLIndexResult;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.Query.FromBuilder;
import shared.database.model.Query.WhereBuilder;
import shared.database.model.graph.SchemaGraph;
import shared.util.Graph;
import shared.util.Pair;
import shared.util.Triplet;
import shared.util.Graph.NoLabel;

/**
 * Generates a query set for the given parameters
 */
public class Generator {

    HashMap<String, List<Triplet<SQLColumn, Integer, String>>> chasedKwsTableMap = new HashMap<>();
    String databaseName = null;
    DatabaseType dbType = null;
    SQLDatabase db = null;
    List<String> hardcodedTables = null;

    Integer queriesNum = null;
    Integer numOfJoins = null;

    boolean pseudoNL = false;
    String fileName = null;
    int[] percentages = { 0, 1, 3, 30, 50 };

    boolean skipNl = false;
    String delim = ";";

    private boolean guarantyJoins = false;

    public static Generator builder() {
        return new Generator();
    }

    public Generator setDB(String databaseName, DatabaseType dbType) {
        this.databaseName = databaseName;
        this.dbType = dbType;
        return this;
    }

    public Generator setQueriesNumber(Integer queriesNum) {
        this.queriesNum = queriesNum;
        return this;
    }

    public Generator numOfJoins(Integer numOfJoins) {
        this.numOfJoins = numOfJoins;
        return this;
    }

    public Generator pseudoNL(boolean pseudoNL) {
        this.pseudoNL = pseudoNL;
        return this;
    }

    public Generator guarantyJoins(boolean guarantyJoins) {
        this.guarantyJoins = guarantyJoins;
        return this;
    }

    public Generator setHardcodedTables(List<String> hardcodedTables) {
        this.hardcodedTables = hardcodedTables;
        return this;
    }

    public Generator toCsv(String fileName, String delimiter, boolean skipNl) {
        this.fileName = fileName;
        this.delim = delimiter;
        this.skipNl = skipNl;
        return this;
    }

    public List<Triplet<String, String, Integer>> gen() {
        if (databaseName == null || dbType == null || queriesNum == null)
            return null;

        if (numOfJoins != null && numOfJoins > 0) {
            return genWithJoins();
        } else {
            return genNoJoins();
        }
    }

    /**
     * Generate queries for an initialized generator object
     *
     * @param databaseName
     * @param queriesNum
     * @param type
     */
    private List<Triplet<String, String, Integer>> genNoJoins() {
        List<Triplet<String, String, Integer>> allQueries = new ArrayList<>();

        // Crete a db object
        db = SQLDatabase.InstantiateDatabase(databaseName, dbType);
        System.out.println("Generate| DB: " + db.getName() + ", Joins: " + numOfJoins + ", QueriesNum: " + queriesNum);

        // Find SQLTables with at least one FULLTEXT indexed column
        List<SQLTable> tables = determineTables();

        // Split the queries num to the tables with the indexed columns
        int qForNextTable = queriesNum / tables.size() + queriesNum % tables.size();
        System.out.println("Split to tables:");
        for (SQLTable table : tables) {
            System.out.println("\tTable: " + table.getName() + " #q= " + qForNextTable);
            List<Triplet<String, String, Integer>> queries = genNlSqlPairs(genKwsForTable(table, qForNextTable, this.percentages));
            allQueries.addAll(queries);

            // prepare the queries number for the next table
            if (queries.size() != qForNextTable)
                qForNextTable = queriesNum / tables.size() + qForNextTable - queries.size();
            else
                qForNextTable = queriesNum / tables.size();
        }


        // Sort based on rows num
        Collections.sort(allQueries, (a, b) -> b.getThird() - a.getThird());

        if (fileName != null) {
            writeToCsv(allQueries);
        }

        // Generate Nl, SQL queries
        return allQueries;
    }

    /**
     * Generate queries for an initialized generator object
     *
     * @param databaseName
     * @param queriesNum
     * @param type
     */
    private List<Triplet<String, String, Integer>> genWithJoins() {
        List<Triplet<String, String, Integer>> allQueries = new ArrayList<>();

        // Crete a db object | Schema Graph
        db = SQLDatabase.InstantiateDatabase(databaseName, dbType);
        SchemaGraph graph = new SchemaGraph();
        graph.fillUnDirected(db.getTables(), db.getFKConstrains());

        // Find SQLTables with at least one FULLTEXT indexed column
        List<SQLTable> tables = determineTables();

        // Create the candidates based on the number of joins between tables
        List<List<SQLTable>> candidates = new ArrayList<>();
        List<List<Pair<SQLColumn, SQLColumn>>> joinHelpersAttrs = new ArrayList<>();
        List<HashSet<SQLTable>> joinHelpersTables = new ArrayList<>();
        for (SQLTable table : tables) {
            List<SQLTable> candidate = new ArrayList<>();
            List<Pair<SQLColumn, SQLColumn>> joinHelperAttrs = new ArrayList<>();
            HashSet<SQLTable> joinHelperTables = new HashSet<>();
            candidate.add(table);
            int joins = 0;

            while (joins < numOfJoins) {
                boolean found = false;

                // Create an integer list with all tables indecies
                Integer[] intArray = new Integer[tables.size()];
                for (int idx=0; idx< tables.size(); idx++)
                    intArray[idx] = idx;
		        List<Integer> intList = Arrays.asList(intArray);
		        Collections.shuffle(intList);

                for (int i=0; i<tables.size(); i++) {
                    SQLTable inner = tables.get( intList.get(i) ); // Get random table
                    SQLTable last = candidate.get(candidate.size() - 1);
                    if (last == inner)
                        continue;

                    // Get the path connecting 2 nodes
                    Integer j = null;
                    Graph<SQLTable, NoLabel> path = graph.getPathConnecting2Nodes(last, inner);
                    if (path == null) j = Integer.MAX_VALUE;
                    else j = path.getVertexes().size();

                    // check joins
                    if (joins + j <= numOfJoins) {
                        // Add all the tables in the path + their auxiliary tables
                        for (Graph<SQLTable, NoLabel>.Edge e: path.getEdges()) {
                            Pair<SQLColumn, SQLColumn> joinAttrs = graph.getColumnsJoiningNodes(
                                e.getStartNode(), e.getEndNode(), null
                            );
                            joinHelperAttrs.add(joinAttrs);
                        }
                        joinHelperTables.addAll(path.getVertexes());

                        candidate.add(inner);
                        joins += j;
                        found = true;
                        break;
                    }
                }

                if (!found)
                    break; // The whole loop went without adding something
            }

            // If num of joins reached then keep it
            if (joins == numOfJoins) {
                boolean foundEqual = false;
                for (List<SQLTable> t : candidates) {
                    if (listEqualsIgnoreOrder(t, candidate)) {
                        foundEqual = true;
                        break;
                    }
                }
                if (!foundEqual) {
                    candidates.add(candidate);
                    joinHelpersAttrs.add(joinHelperAttrs);
                    joinHelpersTables.add(joinHelperTables);
                }
            }
        }

        // Check if there are candidates
        if (candidates.isEmpty()) {
            System.out.println("[ERR] No candidates for db= " + db.getName() + " & joins= " + numOfJoins);
        }
        // If so create queries from them
        else {
            // Split the queries num to each candidate
            int qForNextCandidate = queriesNum / candidates.size() + queriesNum % candidates.size();
            System.out.println("Split to candidates:");
            for (int i =0; i< candidates.size(); i++) {
                List<SQLTable> candidate = candidates.get(i);
                List<Pair<SQLColumn, SQLColumn>> joinHelperAttrs = joinHelpersAttrs.get(i);
                HashSet<SQLTable> joinHelperTables = joinHelpersTables.get(i);

                System.out.println("\tCandidate: {" + candidate.toString() + "} #q= " + qForNextCandidate);

                // Create queries for that candidate
                List<Triplet<String, String, Integer>> queries = genNlSqlPairs2(
                        genQueriesForJoiningTables(candidate, joinHelperAttrs , joinHelperTables, qForNextCandidate + 10, guarantyJoins
                        ));

                allQueries.addAll(queries);
                // prepare the queries number for the next table
                if (queries.size() != qForNextCandidate)
                    qForNextCandidate = queriesNum / candidates.size() + qForNextCandidate - queries.size();
                else
                    qForNextCandidate = queriesNum / candidates.size();
            }
        }

        // Remove the margin of error
        if (allQueries.size() > queriesNum) {
            allQueries.subList(0, queriesNum + 1);
        }

        // Write results to csv
        if (fileName != null) {
            writeToCsv(allQueries);
        }

        // Generate Nl, SQL queries
        return allQueries;
    }

    public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
        return new HashSet<>(list1).equals(new HashSet<>(list2));
    }

    private void writeToCsv(List<Triplet<String, String, Integer>> allQueries) {
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true));
            String str = "Nl, SQL, #, " + db.getName() + " | " + this.numOfJoins + "\n";
            for (Triplet<String, String, Integer> pair : allQueries) {
                str += pair.getFirst() + delim + databaseName + delim + pair.getThird()
                        + (skipNl ? "" : delim + pair.getSecond()) + "\n";
            }

            writer.write(str);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private List<SQLTable> determineTables() {
        List<SQLTable> tables = new ArrayList<>();
        List<SQLTable> tablesToSearch;

        // Take hardCoded tables if defined
        if (hardcodedTables != null) {
            tablesToSearch = new ArrayList<>();
            for (String tableName : hardcodedTables) {
                tablesToSearch.add(db.getTableByName(tableName));
            }
        } else {
            tablesToSearch = db.getTables();
        }

        for (SQLTable table : tablesToSearch) {
            for (SQLColumn col : table.getColumns()) {
                if (col.isIndexed()) {
                    tables.add(table);
                    break;
                }
            }
        }

        return tables;
    }

    /**
     * Create a number of keyword-value pairs matching the parameters
     */
    private List<Triplet<SQLColumn, Integer, String>> genKwsForTable(SQLTable table, Integer numOfKws, int[] percentages) {
        List<Triplet<SQLColumn, Integer, String>> kws = this.chasedKwsTableMap.get(table.getName());
        if (kws == null ) {
            kws = new ArrayList<>();
            System.out.println("Table: " + table);

            // Query the index for the table
            Connection con = null;
            Statement st = null;
            ResultSet rs = null;
            try {
                con = DataSourceFactory.getConnection();

                // Set the table's index
                String query = String.format(SQLQueries.SET_INDEX_TABLE_FOR_SQL_TABLE, db.getName(), table.getName());
                st = con.createStatement();
                st.execute(query);
                st.close();

                // Get index size
                int idxSize = -1;
                query = String.format(SQLQueries.GET_INDEX_SIZE);
                st = con.createStatement();
                rs = st.executeQuery(query);
                if (rs.next()) {
                    idxSize = rs.getInt("cnt");
                }
                rs.close();
                st.close();

                // Get 5 samples out of the zipfian distributed index
                for (int p : percentages) {
                    query = String.format(SQLQueries.RETRIEVE_KW_FROM_IDX_WITH_OFFSET, p * idxSize / 100,
                            numOfKws / percentages.length);
                    st = con.createStatement();
                    rs = st.executeQuery(query);
                    while (rs.next()) {
                        String kw = rs.getString("WORD");
                        SQLColumn column = null;
                        int rows = 0;

                        // Search what column has that kw
                        // for (SQLColumn col : db.getAllColumns()) { //TODO CHANGE
                        for (SQLColumn col : table.getColumns()) {
                            if (!col.isIndexed()) continue;
                            SQLIndexResult res = DatabaseIndexManager.searchKeyword(db, kw, col, false);
                            if (res != null) {
                                column = col;
                                rows += res.getTuples().size();
                            }
                        }

                        // Add the kw
                        kws.add(new Triplet<>(column, rows, rs.getString("WORD")));
                    }
                    rs.close();
                    st.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                DatabaseUtil.close(con, st, rs);
            }

            // Cache the kws
            this.chasedKwsTableMap.put(table.getName(), kws);
        }

        // Create the queries
        return kws;
    }

    /**
     * Create a number of keyword-value pairs matching the parameters
     */
    private List<List<Triplet<SQLColumn, Integer, String>>> genQueriesForJoiningTables(
        List<SQLTable> candidate,
        List<Pair<SQLColumn, SQLColumn>> joinHelperAttrs,
        HashSet<SQLTable> joinHelperTables,
        Integer numOfKws,
        boolean guarantyJoins)
    {
        List<List<Triplet<SQLColumn, Integer, String>>> doubleKws = new ArrayList<>();

        // Find how many columns are indexed per table
        List<List<SQLColumn>> indexedColumns = new ArrayList<>();
        for (SQLTable table : candidate) {
            List<SQLColumn> inCols = new ArrayList<>();
            for (SQLColumn col : table.getColumns()) {
                if (col.isIndexed()) {
                    inCols.add(col);
                }
            }
            indexedColumns.add(inCols);
        }

        // Get kws for those tables
        List<List<Triplet<SQLColumn, Integer, String>>> keywords = new ArrayList<>();
        List<Integer> indexes = new ArrayList<>();
        for (SQLTable table : candidate) {
            List<Triplet<SQLColumn, Integer, String>> kw = new ArrayList<>( genKwsForTable(table, 200, new int[] { 0, 2, 10}) );
            kw.removeIf(triplet -> triplet.getSecond() < 80 || triplet.getThird().length() <= 3);
            Collections.sort(kw, (a, b) -> a.getSecond() - b.getSecond());
            keywords.add(kw);
            indexes.add(0);
        }

        Integer maxSize = 1;
        for (int i=0; i < keywords.size(); i++) {
            System.out.println("Size: " + keywords.get(i).size());
            maxSize *= keywords.get(i).size();
        }

        // Check if kws join
        Connection con = null;
        Statement st = null;
        ResultSet rs = null;
        Integer[] place = new Integer[] {0};
        Integer[] count = new Integer[] {0};
        try {
            con = DataSourceFactory.getConnection();

            // See if the two tables join on rows containing those kws.
            while (true) {
                if (count[0] == maxSize) break;
                List<Triplet<SQLColumn, Integer, String>> perm = genPermute(keywords, indexes, place, count);

                // Create query
                Query q = new Query(db).select("*");
                FromBuilder from = q.from();
                WhereBuilder where = q.where();

                // Just add tales and kws
                if (!guarantyJoins) {
                    int alias=0;
                    for (Triplet<SQLColumn, Integer, String> kw : perm) {
                        from.addTable(kw.getFirst().getTable() + " a" + alias);
                        where.addInvIndexCond("a" + alias++ + "." + kw.getFirst().getName() , kw.getThird());
                    }
                }
                // Add also join constraints
                else {
                    // Create alias map
                    HashMap<SQLTable, String> aliases = new HashMap<>();
                    int alias=0;

                    // Add tables
                    for (SQLTable t: joinHelperTables) {
                        String al = "a" + alias++;
                        aliases.put(t, al);
                        from.addTable(t + " " + al);
                    }

                    // Add constraints
                    for (Pair<SQLColumn, SQLColumn> joinAttr: joinHelperAttrs) {
                        where.addJoinCond(
                            aliases.get(joinAttr.getLeft().getTable()) + "." + joinAttr.getLeft().getName(),
                            aliases.get(joinAttr.getRight().getTable()) + "." + joinAttr.getRight().getName());
                    }

                    // Add keyword constraints
                    for (Triplet<SQLColumn, Integer, String> kw : perm) {
                        where.addInvIndexCond(aliases.get(kw.getFirst().getTable()) + "." + kw.getFirst().getName() , kw.getThird());
                    }
                }
                from.endFrom();
                where.endWhere();
                q.limit(1);

                st = con.createStatement();
                System.out.println("Query: " + q.toSQL());
                rs = st.executeQuery(q.toSQL());

                // Add to double kws
                if (rs.first()) {
                    doubleKws.add(perm);
                }

                if (numOfKws == doubleKws.size()) {
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DatabaseUtil.close(con, st, rs);
        }

        // Create the queries
        return doubleKws;
    }

    private List<Triplet<SQLColumn, Integer, String>> genPermute(List<List<Triplet<SQLColumn, Integer, String>>> keywords, List<Integer> idx, Integer[] place, Integer[] count) {
        List<Triplet<SQLColumn, Integer, String>> perm = new ArrayList<>();
        for (int i=0; i < keywords.size(); i++) {
            perm.add( keywords.get(i).get( idx.get(i) ) );
            count[0]++;
        }

        // Update index if capable to
        if (idx.get(place[0]) == keywords.get(place[0]).size() - 1) {
            idx.set(place[0], 0);
        } else {
            idx.set(place[0], idx.get(place[0]) + 1);
        }


        // Roll the place of the updates
        place[0]++;  // Go to the next the next time
        if (place[0] == keywords.size()) {
            place[0] = 0;
        }

        return perm;
    }


    /**
     * Generate NP, SQL pairs  from a list of kws
     *
     * @param kws
     * @return
     */
    private List<Triplet<String, String, Integer>> genNlSqlPairs2(List<List<Triplet<SQLColumn, Integer, String>>> kws) {
        List<Triplet<String, String, Integer>> queries = new ArrayList<>();

        // Loop all kws
        for (List<Triplet<SQLColumn, Integer, String>> perm: kws) {

            // Create an nl, sql query for that kw
            String nl = ( pseudoNL ? "Find about " : "" );
            Query q = new Query(db).select("*");
            FromBuilder from = q.from();
            WhereBuilder where = q.where();
            Integer avgFreq = 0;

            int alias=0;
            for (Triplet<SQLColumn, Integer, String> kw: perm) {
                nl += kw.getThird() + (pseudoNL ? " and " : " ");
                from.addTable(kw.getFirst().getTable() + " a" + alias);
                where.addInvIndexCond("a" + alias++ + "." + kw.getFirst().getName() , kw.getThird());
                avgFreq += kw.getSecond();
            }
            from.endFrom();
            where.endWhere();
            q.limit(1);

            // Remove last from nl
            nl.substring(0, nl.length() - (pseudoNL ? 5 : 1) );
            queries.add(new Triplet<>(nl, q.toSQL(), avgFreq/perm.size()) );
        }

        return queries;
    }

    private List<Triplet<String, String, Integer>> genNlSqlPairs(List<Triplet<SQLColumn, Integer, String>> kws) {
        List<Triplet<String, String, Integer>> queries = new ArrayList<>();

        // Loop all kws
        for (Triplet<SQLColumn, Integer, String> pair: kws) {
            if (pair.containsNullObject()) continue;

            // Create an nl, sql query for that kw
            queries.add( new Triplet<>(
                ( pseudoNL ? "Find about " : "" ) + pair.getThird(),
                new Query(db).select("*").from(pair.getFirst().getTableName()).where().addInvIndexCond(pair.getFirst(), pair.getThird()).endWhere().toSQL(),
                pair.getSecond()
            ));
        }
        return queries;
    }

    /**
     * This is a SAMPLE main, on how to use the generator
     */
    public static void main(String[] args) {
        PropertiesSingleton.loadPropertiesFile("app");
        Generator g = new Generator();
        List<Triplet<String, String, Integer>> qs = new ArrayList<>();


        // Loop all 3 databases to create queries
        HashMap<String, List<String>> dbs = new HashMap<>();
        dbs.put("IMDB", Arrays.asList("movie", "director", "actor"));
        dbs.put("MAS", Arrays.asList("organization", "author", "conference", "journal", "domain"));
        dbs.put("YELP", Arrays.asList("business", "category", "user"));

        for (Entry<String, List<String>> entry: dbs.entrySet()) {
            // Create agenerator per db to enable internal caching
            g.setDB(entry.getKey(), DatabaseType.MySQL)
                 .setQueriesNumber(25)
                //  .guarantyJoins(true)
                 .setHardcodedTables(entry.getValue())
                 .toCsv("queries.csv", ";", true);

            for (int i = 5; i <= 5; i++) {
                g.numOfJoins(i);
                qs.addAll(g.gen());
            }
        }

        // for (Triplet<String, String, Integer> p: qs) {
        //     System.out.println("Q: " + p.getFirst() + " | SQL: " + p.getSecond() + " | Rows: " + p.getThird());
        // }
    }

}
