package discover.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import discover.model.execution.JoinableFormat;
import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.util.Pair;

// An SQLTempTable represents a temporary table (view) created in the 
// underling SQL Database to store an intermediate result that 
// is widely used by the Executor class to compute the final results. 
// All those tables are Dumped at the end of the Plan's Execution.
// An SQLTempTable contains only columns and PrimaryKeys of the table
// creating it.
public class SQLTempTable extends SQLTable {
    
    // What tables are joined to create this TempTable
    private List<SQLTable> containedTables;

    // A HashMap containing the SQLTables contained by this TempTable
    // and how their columns are matched with this.TempTableColumns.
    private HashMap<SQLTable, HashMap<SQLColumn, List<SQLColumn>>> baseTableColumnLinksMap;

    // Public Constructor    
    public SQLTempTable(String name) {
        super(name);
        this.containedTables = new ArrayList<>();
        this.baseTableColumnLinksMap = new HashMap<>();   
    }

    // Return the table's name without the auxiliary database prefix
    public String getNameWithoutAuxDB() {
        return this.name.split("\\.")[1];
    }
     
    // Returns a List of SQLTables used to create this TempTable.
    @Override    
    public List<SQLTable> getContainedTables() {        
        return this.containedTables;
    }

    // Get the TempTable's SQLColumn that was created like the 
    // SQLColumn column that the SQLTable containedTable has stored.
    public SQLColumn getColumnFromContainedTableByName(SQLTable containedTable, SQLColumn column) {        
        return this.baseTableColumnLinksMap.get(containedTable).get(column).get(0);
    }

    // Get the Contained SQLTable and its Column from the HashMap baseTableToColumnsMap with 
    // the SQLColumn cloned and updated for this TempTable.
    public Pair<SQLTable, SQLColumn> getBaseTableAndColumn(SQLColumn tempTableColumn) {
        List<Pair<SQLTable,SQLColumn>> possibleRetValues = new ArrayList<>();

        // To find this we need to fully loop the map.
        for(Map.Entry<SQLTable, HashMap<SQLColumn, List<SQLColumn>>> entry: this.baseTableColumnLinksMap.entrySet())
            for (Map.Entry<SQLColumn, List<SQLColumn>> innerEntry: entry.getValue().entrySet()) 
                for (SQLColumn col: innerEntry.getValue()) 
                    if (col.equals(tempTableColumn))
                        possibleRetValues.add(new Pair<SQLTable,SQLColumn>(entry.getKey(), innerEntry.getKey()));
               
        if (!possibleRetValues.isEmpty())
            for (Pair<SQLTable,SQLColumn> p: possibleRetValues)
                if (!p.getLeft().getName().contains("thor_db"))  // Change hard codded
                    return p;
        return null;        
    }
   
    // Fill this tempTable using a JoinableFormat.
    public void fill(JoinableFormat joinableFormat) {
        HashMap<String, Integer> tableApps = new HashMap<>();  // Stores table names along with number of appearances in joinableFormat.

        // Fill the Contained Table field from the joinableFormat's tablesToJoin.
        for(Pair<SQLTable, String> pair: joinableFormat.getTablesToJoin()) {
            this.containedTables.addAll(pair.getLeft().getContainedTables());

            // Store the tables in a map along with an integer indicating their
            // appearance in the contained Table list.
            for (SQLTable table: pair.getLeft().getContainedTables()) {
                tableApps.put(table.getName(), tableApps.getOrDefault(table.getName(), 0) + 1);
            }
        }

        // Remove the tables with 1 appearance
        Iterator<Map.Entry<String, Integer>> iterator = tableApps.entrySet().iterator();  
        while (iterator.hasNext()) { 
            Map.Entry<String, Integer> entry = iterator.next(); 
            if (entry.getValue() == 1)
                iterator.remove(); 
        }


        // Then clone each column from the joinableFormat's columnsFromTables changing its 
        // name to [table]_[column] and updating the column's Table field to point to this.
        // Also update the HashMap linking base Tables (SQLTables from the database) and their
        // columns with this tempTables columns that where inherited by them.        
        for (Pair<SQLColumn, String> pair: joinableFormat.getColumnsFromTables()) {
            SQLColumn column = pair.getLeft();
            SQLColumn cloneColumn = new SQLColumn(column);            
            // If column comes from an SQLTempTable dont change it's name. On both cases
            // update the hash map.
            if (column.getTable() instanceof SQLTempTable) {                
                this.updateHashMap(cloneColumn, column, (SQLTempTable) column.getTable());
            } 
            else {
                // Create the columns Name and take care of columns coming 
                // from tables with the same name.
                String columnName = this.getNameWithoutAuxDB() + column.getTableName() + "_" + column.getName();
                if (tableApps.get(column.getTableName()) != null && tableApps.get(column.getTableName()) > 1)
                    columnName += "_" + pair.getRight();
                cloneColumn.setName(columnName);
                this.updateHashMap(cloneColumn, column, column.getTable());
            }
            cloneColumn.setTable(this);
            this.addColumn(cloneColumn);
        }

        // System.out.print("Debug Print TempTable");
        // this.debugPrint();
        // System.out.println();
    }  
    

    public void fillTableLikeExistingTable(SQLTable table) {
        // Fill the Contained Table field from the table parameter.        
        this.containedTables.addAll(table.getContainedTables());
        
        // Then clone each column from the SQLTable's columns updating the column's Table field
        // to point to this. Also update the HashMap linking base Tables (SQLTables from the database)
        // and their columns with this tempTables columns that where inherited by them.
        for (SQLColumn column: table.getColumns()) {            
            SQLColumn cloneColumn = new SQLColumn(column);                        
            this.updateHashMap(cloneColumn, column, column.getTable());
            cloneColumn.setTable(this);
            this.addColumn(cloneColumn);
        }

        // System.out.println("OLD TABLE");
        // table.debugPrint();
        // System.out.println();
        // System.out.println("NEW TABLE");        
        // this.debugPrint();
        // System.out.println();                
    }

    // Update the baseTablesToColumnMap with the new clonedColumn that is going to be 
    // add tou this.columns. The new clonedColumn must be connected with a baseTables's
    // column. The clonesColumn is a clone of the irTablesColumn. The irTableUsedForCreation
    // and the irTablesColumn will help link the clone column wih a baseTable.
    public void updateHashMap(SQLColumn clonedColumn, SQLColumn irTablesColumn, SQLTempTable irTableUsedForCreation) {
        // Get the baseTable (SQLTable) and its Column which is like irTablesColumn.
        Pair<SQLTable, SQLColumn> baseTableAndColumn = irTableUsedForCreation.getBaseTableAndColumn(irTablesColumn);


        // If the base table is already in this HashMap then append the hashMap with another column link.
        HashMap<SQLColumn, List<SQLColumn>> columnsLinks = this.baseTableColumnLinksMap.get(baseTableAndColumn.getLeft());
        if (columnsLinks != null) {
            List<SQLColumn> exList = columnsLinks.get(baseTableAndColumn.getRight());
            if (exList != null)
                exList.add(clonedColumn);
            else {                
                List<SQLColumn> newList = new ArrayList<SQLColumn>();
                newList.add(clonedColumn);
                columnsLinks.put(baseTableAndColumn.getRight(), newList);                
            }            
        }
        else {
            // Else add a new link between base table and its columns.
            columnsLinks = new HashMap<>();
            List<SQLColumn> newList = new ArrayList<SQLColumn>();
            newList.add(clonedColumn);
            columnsLinks.put(baseTableAndColumn.getRight(), newList);            
            this.baseTableColumnLinksMap.put(baseTableAndColumn.getLeft(), columnsLinks);
        }


        // Also add links for the SQLTemp table and not only their base tables (this is for creating the views)
        columnsLinks = this.baseTableColumnLinksMap.get(irTableUsedForCreation);
        if (columnsLinks != null) {
            List<SQLColumn> exList = columnsLinks.get(irTablesColumn);
            if (exList != null)
                exList.add(clonedColumn);
            else {                
                List<SQLColumn> newList = new ArrayList<SQLColumn>();
                newList.add(clonedColumn);
                columnsLinks.put(irTablesColumn, newList);                
            } 
        }
        else {
            // Else add a new link between base table and its columns.
            columnsLinks = new HashMap<>();
            List<SQLColumn> newList = new ArrayList<SQLColumn>();
            newList.add(clonedColumn);
            columnsLinks.put(irTablesColumn, newList);
            this.baseTableColumnLinksMap.put(irTableUsedForCreation, columnsLinks);
        }
    }

    // Update the HashMap with a baseTable (SQLTable) and its baseColumn
    public void updateHashMap(SQLColumn clonedColumn, SQLColumn baseColumn, SQLTable baseTable) {        
        // If the base table is already in this HashMap then append the hashMap with another column link.
        HashMap<SQLColumn, List<SQLColumn>> columnsLinks = this.baseTableColumnLinksMap.get(baseTable);
        if (columnsLinks != null) {
            List<SQLColumn> exList = columnsLinks.get(baseColumn);
            if (exList != null)
                exList.add(clonedColumn);
            else {                
                List<SQLColumn> newList = new ArrayList<SQLColumn>();
                newList.add(clonedColumn);
                columnsLinks.put(baseColumn, newList);                
            }            
        }
        else {
            // Else add a new link between base table and its columns.            
            columnsLinks = new HashMap<>();
            List<SQLColumn> newList = new ArrayList<SQLColumn>();
            newList.add(clonedColumn);
            columnsLinks.put(baseColumn, newList);
            this.baseTableColumnLinksMap.put(baseTable, columnsLinks);            
        }
    }

}