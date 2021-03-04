package shared.database .model;

import java.util.ArrayList;
import java.util.List;

import shared.database .interfaces.SchemaElement;

// This class models a SQL column.
public class SQLColumn implements SchemaElement {

    public static final String PK_IDENTIFIER = "PRI";
    public static final String FK_IDENTIFIER = "FOR";
    public static final String MUL_IDENTIFIER = "MUL";


    private SQLTable table;       // The name of the table the column belongs to.
    private String name;          // The column name.
    private SQLType type;         // The column type.
    private List<String> key;     // The column key. Show if the column is PK or FK or both
    private boolean isIndexed;    // Indicates whether the column is indexed or not.
    private double averageLength; // The average length of the column's values in words.

    public SQLColumn(SQLTable table, String name, SQLType type, String key) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.key  = new ArrayList<>();
        this.key.add(key);
        this.isIndexed = false; // It will be set latter
        this.averageLength = 0.0;
    }

    public SQLColumn(SQLTable table, String name, SQLType type) {
        this.table = table;
        this.name = name;
        this.type = type;
        this.key  = new ArrayList<>();        
        this.isIndexed = false; // It will be set latter
        this.averageLength = 0.0;
    }

    // Copy Constructor.
    public SQLColumn(SQLColumn column) {
        this.table = column.table;
        this.name = column.name;
        this.type = column.type;
        this.key = new ArrayList<>(column.key);
        this.isIndexed = column.isIndexed;
        this.averageLength = column.averageLength;
    }

    // Getters and Setters.
    public SQLTable getTable() {
        return table;
    }

    public String getTableName() {
        return this.table.getName();
    }

    public void setTable(SQLTable table) {
        this.table = table;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SQLType getType() {
        return this.type;
    }

    public void setType(String type, Integer maximumLength) {
        this.type = new SQLType(type, maximumLength);
    }

    public List<String> getKey() {
        return key;
    }

    public void addKey(String key) {
        this.key.add(key);
    }

    public void setIsIndexed(boolean isIndexed) {
        this.isIndexed = isIndexed;
    }

    public double getAverageLength() {
        return this.averageLength;
    }

    public void setAverageLength(double averageLength) {
        this.averageLength = averageLength;
    }

    // Returns true if the column is part of the tables with tableName Primary Key.
    public boolean isPartOfPrimaryKey() {
        return this.isPrimary();
    }

    // Returns true if the database is indexed.
    public boolean isIndexed() {
        return this.isIndexed;
    }

    public boolean isPartOfMulIndex() {
        return this.key.indexOf(MUL_IDENTIFIER) != -1;
    }

    // Returns true if the column is part of a table's primary key.
    public boolean isPrimary() {
        return this.key.indexOf(PK_IDENTIFIER) != -1;
    }

    // Returns true if the column is part of a table's primary key.
    public boolean isForeign() {
        return this.key.indexOf(FK_IDENTIFIER) != -1;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + ((this.table != null) ? table.getName().hashCode() : 0);
        hash = 31 * hash + ((this.name != null) ? name.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if(!(obj instanceof SQLColumn)) return false;

        SQLColumn c = (SQLColumn) obj;
        return c.name.equals(this.name) && c.table.getName().equals(this.table.getName());
    }

    @Override
    public String toString() {
        return (this.table != null ? this.getTableName() + "." : "") + this.name;
    }

}
