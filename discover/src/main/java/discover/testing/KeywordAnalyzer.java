package discover.testing;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
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

import discover.components.Parser;
import shared.database.config.PropertiesSingleton;
import shared.database.connectivity.DatabaseUtil;
import shared.database.connectivity.DataSourceFactory;
import shared.database.model.DatabaseType;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import shared.database.model.graph.SchemaGraph;


public class KeywordAnalyzer {

    private static String INPUT_FILE = "qc";

    public static void main(String[] args) {
        // Load the configurations
        PropertiesSingleton.loadPropertiesFile("app");

        // extractQueryStats();
        extractKeywordStats();
        
    }

    public static void extractQueryStats(){
        List<String> csvLines = new ArrayList<>();
        String[] sl = { "COUNT", "MAX", "MIN", "SUM", "AVG", "GROUPBY",
                        "count", "max", "min", "sum", "avg", "groupby" };
        Set<String> stopw = new HashSet<String>( Arrays.asList(sl) );        

        for (String ovQuery: TestingMain.getQueryList(INPUT_FILE)) {

            // Get the query and the db
            String query = ovQuery.split(";")[0];
            String schemaName = ovQuery.split(";")[1];

            System.out.println("[INFO] Executing: " + query + "...");
    
            // Load the db.            
            String name = schemaName.split("\\.")[1];
            String type = schemaName.split("\\.")[0];

            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(name, shared.database.model.DatabaseType.getTypeFromString(type));
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillUnDirected(database.getTables(), database.getFKConstrains());

            // Remove the metadata from the queries:            
            List<String> keywords = Parser.whitespaceTokenizer(query);
            keywords.removeIf( word -> (stopw.contains(word)) );  // first remove the COUNT operators
            Integer numOfAllKws = keywords.size();
            keywords.removeIf( word -> {
                // If word is metadata term then return true
                for(SQLTable table: database.getTables())
                    if (table.getName().equals(word))
                        return true;
                    else if ( table.getColumnByName(word) != null)
                        return true;                                            
                
                // Else return false
                return false;
            });
            
            System.out.println("\tKws: " + keywords + "...");

            // Find kw Appearances in columns            
            HashMap<String, Integer> kwApp = new HashMap<>();            
            Double avgDist = findKWAppearances(keywords, database, schemaGraph, kwApp);

            // Create the str to wite in the scv
            String str = query +  ", " + keywords.size() + ", " +  (numOfAllKws - keywords.size()) + ", ";
            Integer sumMappings = 0;
            Double avgMappings = 0D;            
            String mapStr = "{";
            for (Map.Entry<String, Integer> entry: kwApp.entrySet()) {
                sumMappings += entry.getValue();
                mapStr += entry.getKey() + " : " + entry.getValue() + " | ";
            }          

            if (mapStr.length() > 1)
                mapStr = mapStr.substring(0, mapStr.length() - 3); // remove last ", "
            mapStr+= "}";
            if (keywords.size() != 0)
                avgMappings = ((double)sumMappings) / (int) keywords.size() ;

            str += mapStr + ", " + sumMappings + ", " + avgMappings + ", " + avgDist  + ", " + schemaName;
            csvLines.add(str);
        }

        // Write results to a file
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("querystats.csv"));
            writer.write("query, #ofKWs, #ofMetadata, #MappingsPerKw, SumMappings, AvgMappings, AvgDistOfMappings, Schema\n");            
            for (String csv: csvLines)
                writer.write(csv + "\n");
            writer.close();
        }
        catch(IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    public static void extractKeywordStats() {        
        String[] sl = { "COUNT", "MAX", "MIN", "SUM", "AVG", "GROUPBY",
                        "count", "max", "min", "sum", "avg", "groupby" };
        Set<String> stopw = new HashSet<String>( Arrays.asList(sl) );
        HashMap<String, HashMap<String, String[]>> keywordPerDb = new HashMap<>();
        
        for (String ovQuery: TestingMain.getQueryList("qc")) {

            // Get the query and the db
            String query = ovQuery.split(";")[0];
            String schemaName = ovQuery.split(";")[1];

            System.out.println("[INFO] Executing: " + query + "...");
    
            // Load the db.
            String name = schemaName.split("\\.")[1];
            String type = schemaName.split("\\.")[0];

            // Automatically get the info of database.
            SQLDatabase database = SQLDatabase.InstantiateDatabase(name, DatabaseType.getTypeFromString(type));
            SchemaGraph schemaGraph = new SchemaGraph();
            schemaGraph.fillUnDirected(database.getTables(), database.getFKConstrains());      

            // Remove the metadata from the queries:            
            List<String> keywords = Parser.whitespaceTokenizer(query);
            keywords.removeIf( word -> (stopw.contains(word)) );  // first remove the COUNT operators            
            keywords.removeIf( word -> {
                // If word is metadata term then return true
                for(SQLTable table: database.getTables())
                    if (table.getName().equals(word))
                        return true;
                    else if ( table.getColumnByName(word) != null)
                        return true;                                            
                
                // Else return false
                return false;
            });
            
            // Add database map if not exists
            HashMap<String, String[]> kwApp = keywordPerDb.get(schemaName);
            if (kwApp == null) {
                kwApp = new HashMap<String, String[]>();
                keywordPerDb.put(schemaName, kwApp);
            }

            // Find kw Appearances in columns if not already found
            for (String kw: keywords)
                if (kwApp.get(kw) == null)
                    kwApp.put(kw, findKWAppearancesWithDist(kw, database, schemaGraph));
            
        }
        
        // Write results to a file
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("kwstats.csv"));
            writer.write("Keyword, #ofTables, #OfColumns, AvgDistOfMappings, Schema\n");            
            for (Map.Entry<String, HashMap<String, String[]>> dbMap: keywordPerDb.entrySet())
                for (Map.Entry<String, String[]> kw: dbMap.getValue().entrySet())
                    writer.write(kw.getKey() + ", " +  kw.getValue()[0] + ", " + kw.getValue()[1] + ", " +  kw.getValue()[2] + ", " + dbMap.getKey() + "\n");
            writer.close();
        }
        catch(IOException e) {
            System.out.println("[ERROR] Could not create output file");
            e.printStackTrace();
        }
    }

    /**
     * Return the number of columns in which kw appeared
     * 
     * @param kw
     * @return
     */
    static Double findKWAppearances(List<String> keywords, SQLDatabase database, SchemaGraph graph, HashMap<String, Integer> kwMap) {

        // Initialize variables.        
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        List<List<SQLTable>> mappingsPerKw = new ArrayList<>();

        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();            
            for (String kw: keywords) {
                List<SQLTable> relations = new ArrayList<>();
                Integer kwAppearances = 0;
                
                for (SQLTable table : database.getTables()) {                
                    // Get the num of columns in witch kw appeared
                    int size = getAppearancesInColumns(kw, table, con, stmt, rs);
                    if (size != 0) {
                        relations.add(table);
                        kwAppearances += size;
                    }                    
                }

                kwMap.put(kw, kwAppearances);
                mappingsPerKw.add(relations);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection.
            DatabaseUtil.close(con, stmt, rs);
        }

        // Print mappings 
        Integer c = 1, t = 0;
        for (List<SQLTable> mappings: mappingsPerKw) {
            c *= mappings.size();
            t += mappings.size();
        }
        System.out.println("Found in: " + t + " tables with: " + c + " combos");
        
        Double avgDist = 0D;
        Integer combosNum = 0;
        int[] nextIdxs = new int[mappingsPerKw.size()];
        while(true) {
            Set<SQLTable> curCombo = new HashSet<>();  // The current combo to create            
            for(int idx = 0; idx < mappingsPerKw.size(); idx++)
                curCombo.add(mappingsPerKw.get(idx).get(nextIdxs[idx]));
            
            // System.out.println("Combo " + combosNum + " : " + curCombo);
           
            // Find the distance of the sub graph connecting all the nodes in the combo
            // if (curCombo.size() > 1) {
                avgDist += (double) graph.subGraph(curCombo).getEdges().size();
                combosNum++;
            // }

            // Check for finalizing condition
            int allMappingsReachedSize = 0;
            for (int i=nextIdxs.length - 1; i>=0; i--)
                if(mappingsPerKw.get(i).size() - 1 == nextIdxs[i])  // Size - 1 to reach the last Index
                    allMappingsReachedSize++;

            // We have created all the combos
            if (allMappingsReachedSize == mappingsPerKw.size())
                break;

            // Update index dictionary
            for (int i=nextIdxs.length - 1; i>=0; i--)
                if(mappingsPerKw.get(i).size() - 1 != nextIdxs[i]) { // if sized not equal with the last idx then simple add one more to change the combo from the end to the start ;)
                    nextIdxs[i]++;
                    break;
                } 
                else {  // Make the idx of i go to zero and go back adding 1 , if they reach size too go back again till it stops
                    nextIdxs[i] = 0;
                    boolean breakFlag = false;
                    for (int j=i-1; j>=0; j--) {                        
                        if(mappingsPerKw.get(j).size() - 1== nextIdxs[j]) {
                            nextIdxs[j] = 0;                        
                        }
                        else {
                            nextIdxs[j]++;
                            breakFlag = true;
                            break;
                        }                        
                    }
                    if (breakFlag)
                        break;
                }
        }


        // Return the basicTupleSets.
        return avgDist / combosNum;
    }
    


    // Returns the tuples of a table in which a given keyword was found.
    static Integer getAppearancesInColumns(
        String keyword, SQLTable table,
        Connection con, PreparedStatement stmt,
        ResultSet rs) throws SQLException     
    {
        int appearances = 0;
        // Execute the query against each column's index for the keyword.
        for (SQLColumn column : table.getColumns()) {
            // FULLTEXT indexes can only be built on columns with strings as values.
            if (column.getType().isTextual() && column.isIndexed()) {
                // Prepare the query to execute.
                String query = String.format(SQLQueries.INV_INDEX_QUERY, "*", table.getName(), column.getName());
                stmt = con.prepareStatement(query,
                    ResultSet.TYPE_SCROLL_INSENSITIVE, 
                    ResultSet.CONCUR_READ_ONLY);
                stmt.setString(1, DatabaseUtil.prepareForAndBooleanSearch(keyword));                

                // Execute the query and store the tuples.
                rs = stmt.executeQuery();
                if (rs.next() != false)
                    appearances++;
            }
        }

        // Return the tuples.
        return appearances;
    }

    /**
     * Return the number of columns in which kw appeared
     * 
     * @param kw
     * @return
     */
    static String[] findKWAppearancesWithDist(String kw, SQLDatabase database, SchemaGraph graph) {

        // Initialize variables.
        String[] retVal = new String[3];
        Integer kwAppearances = 0, numOfTables = 0;
        List<String> tables = new ArrayList<>();
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            // Get the connection.
            con = DataSourceFactory.getConnection();            
            for (SQLTable table : database.getTables()) {                
                // Get the num of columns in witch kw appeared
                Integer n = getAppearancesInColumns(kw, table, con, stmt, rs);
                if (n != 0) {
                    numOfTables++;
                    tables.add(table.getName());
                    kwAppearances += n;
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            // Close the connection.
            DatabaseUtil.close(con, stmt, rs);
        }

        // Compute avg distance for keywords in the db
        Set<Integer> checksums = new HashSet<>();
        List<Integer> dist = new ArrayList<>();
        for (int i = 0; i < tables.size(); i++) {
            for (int j = 0; j < tables.size(); j++) {
                if (i == j) continue;
                Integer sum = tables.get(i).hashCode() + tables.get(j).hashCode();
                if (checksums.contains(sum)) continue;
                else checksums.add(sum);

                dist.add(graph.getDistanceBetweenNodes(tables.get(i), tables.get(j)));
            }
        }

        // Calc avg dist
        Double avgdist = 0D;
        for (Integer i: dist)
            avgdist += i;
        if (!dist.isEmpty())
            avgdist = ((double) avgdist) / (int) dist.size();


        // Return the basicTupleSets.
        retVal[0] = numOfTables.toString();
        retVal[1] = kwAppearances.toString();
        retVal[2] = avgdist.toString();
        
        return retVal;
    }

}