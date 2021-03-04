package spark.model;

import java.util.HashMap;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.database.model.SQLType;
import spark.model.TupleSet;


// A temp table is a view of an SQLTable from the database containing only 
// those tuples that the IREngine found they contain query keywords.
public class SQLTempTable extends SQLTable {
    
    // Store the Table that this TempTable is connected with
    private SQLTable baseTable;

    // A hashMap linking baseTable's columns with this table's Columns.
    private HashMap<SQLColumn,SQLColumn> baseTableColumnLinksMap;


    // Public Constructor    
    public SQLTempTable(String name) {
        super(name);        
        this.baseTableColumnLinksMap = new HashMap<>();   
    }
    
    // Returns a List of SQLTables used to create this TempTable.
    public SQLTable getConnectedTable() {        
        return this.baseTable;
    }

    // Get the TempTable's SQLColumn that was created like the 
    // SQLColumn column that the SQLTable containedTable has stored.
    public SQLColumn getColumnLikeConnectedTables(SQLColumn column) {
        return this.baseTableColumnLinksMap.get(column);
    }    
       
    
    // Fills a temp table like the Table connected with the parameter TupleSet.
    public void fill(TupleSet tupleSet) {
        // Get the table from the tupleSet.
        SQLTable table = tupleSet.getTable();

        // Fill the Contained Table field from the table parameter.        
        this.baseTable = table;
        
        // Then clone each column from the SQLTable's columns updating the column's Table field
        // to point to this. Also update the HashMap linking base Tables (SQLTables from the database)
        // and their columns with this tempTables columns that where inherited by them.
        for (SQLColumn column: table.getColumns()) {            
            SQLColumn cloneColumn = new SQLColumn(column);                        
            this.baseTableColumnLinksMap.put(column, cloneColumn);
            cloneColumn.setTable(this);
            this.addColumn(cloneColumn);
        }

        // Add a score column.
        this.addColumn(new SQLColumn(this, "score", new SQLType("double", 0), ""));

        // System.out.println("OLD TABLE");
        // table.debugPrint();
        // System.out.println();
        // System.out.println("NEW TABLE");        
        // this.debugPrint();
        // System.out.println();                
    }

    @Override
    public String toAbbreviation() {
        return this.baseTable.getName();
    }
}