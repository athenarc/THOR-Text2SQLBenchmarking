package shared.database .model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.database .interfaces.SchemaElement;

// This class models a SQL table.
public class SQLTable implements SchemaElement{
    
    protected String name;                 // The table name.    
    protected List<SQLColumn> columns;     // List of columns the table contains.
    protected Set<SQLColumn> primaryKey;   // List of table's columns that compose its primary key.
    protected Set<SQLColumn> foreignKeys;  // List of table's foreign keys.
    protected int rowsNum;                 // Number of rows in the table.

    // Foreign key constraints involving this SQLTable.
    protected List<SQLForeignKeyConstraint> referencingConstraints;
    protected List<SQLForeignKeyConstraint> referencedConstraints;

    public SQLTable(String name) {        
        this.name = name;
        this.columns = new ArrayList<SQLColumn>();
        this.primaryKey = new HashSet<SQLColumn>();
        this.foreignKeys = new HashSet<SQLColumn>();
        this.rowsNum = 0;
        this.referencingConstraints = new ArrayList<SQLForeignKeyConstraint>();
        this.referencedConstraints = new ArrayList<SQLForeignKeyConstraint>();
    }

    // Shallow copy constructor.
    public SQLTable(SQLTable table) {        
        this.name = table.name;
        this.columns = table.columns;
        this.primaryKey = table.primaryKey;
        this.foreignKeys = table.foreignKeys;
        this.rowsNum = table.rowsNum;
        this.referencingConstraints = new ArrayList<SQLForeignKeyConstraint>();
        this.referencedConstraints = new ArrayList<SQLForeignKeyConstraint>();
    }

    // USED BY DISCOVER Returns this in a list of size 1.
    public List<SQLTable> getContainedTables() {
        List<SQLTable> table = new ArrayList<>();
        table.add(this);
        return table;
    }

    // Getters and Setters.    
    public String getName() { return this.name; }    
    public List<SQLColumn> getColumns() { return this.columns; }
    public Set<SQLColumn>  getForeignKeys() { return this.foreignKeys;}
    public Set<SQLColumn>  getPrimaryKey() { return this.primaryKey; }
    public List<SQLForeignKeyConstraint> getReferencingConstraints() { return this.referencingConstraints; }
    public List<SQLForeignKeyConstraint> getReferencedConstraints() { return this.referencedConstraints; }
         
    public void setName(String name) { this.name = name; }
    public void setPrimaryKey(Set<SQLColumn> primaryKey) { this.primaryKey = primaryKey; }
    public void setReferencingConstraints(List<SQLForeignKeyConstraint> referencingConstraints) { this.referencingConstraints = referencingConstraints; }
    public void setReferencedConstraints(List<SQLForeignKeyConstraint> referencedConstraints) { this.referencedConstraints = referencedConstraints; }
    public void setColumns(List<SQLColumn> columns) { this.columns = columns; }
    public void setForeignKeys(Set<SQLColumn> foreignKeys) { this.foreignKeys = foreignKeys; }
    public int  getRowsNum() { return this.rowsNum; }
    public void setRowsNum(int rowsNum) { this.rowsNum = rowsNum; }

    // Adds a column(s) to the list of foreign keys.
    public void addForeignKey(SQLColumn foreignKey) { this.foreignKeys.add(foreignKey);}
    public void addAllForeignKeys(List<SQLColumn> foreignKeys) { this.foreignKeys.addAll(foreignKeys); }

    // Adds a column(s) to the list of primary keys.
    public void addPrimaryKey(SQLColumn primaryKey) { this.primaryKey.add(primaryKey); }
    public void addPrimaryKey(List<SQLColumn> primaryKey) { this.primaryKey.addAll(primaryKey);  }

    // Add referenced or referencing constraints.
    public void addReferencingForeignKeyConstrain(SQLForeignKeyConstraint constraint) { this.referencingConstraints.add(constraint);}
    public void addReferencedForeignKeyConstrain(SQLForeignKeyConstraint constraint) { this.referencedConstraints.add(constraint);}
    public void addAllReferencingForeignKeyConstraints(List<SQLForeignKeyConstraint> constraints) { this.referencingConstraints.addAll(constraints);}
    public void addAllReferencedForeignKeyConstraints(List<SQLForeignKeyConstraint> constraints) { this.referencedConstraints.addAll(constraints);}


    // Returns true if Table contains a column with that Name.
    public boolean containsColumnWithName(String columnName) {
        for (SQLColumn column: this.columns) {
            if (column.getName().equals(columnName)) {
                return true;
            }
        }

        return false;
    }

    // Returns a list with the columns of the table that are indexed.
    public List<SQLColumn> getIndexedColumns() {
        List<SQLColumn> indexedColumns = new ArrayList<SQLColumn>();
        for (SQLColumn column : this.columns) {
            if (column.isIndexed()) {
                indexedColumns.add(column);
            }
        }

        return indexedColumns;
    }

    // Removes a column from the foreign keys list.
    public void removeForeignKey(SQLColumn column) {
        this.foreignKeys.remove(column);
    }

    // Get the column's names that form this Table's Primary Key.
    public List<String> getPrimaryKeyToString() {
        List<String> columnNames = new ArrayList<String>();
        for (SQLColumn column: this.primaryKey) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    // Get the column's names of the Table's Foreign Keys.
    public List<String> getForeignKeysToString() {
        List<String> columnNames = new ArrayList<String>();
        for (SQLColumn column: this.foreignKeys) {
            columnNames.add(column.getName());
        }
        return columnNames;
    }

    // Removes a referencing constrain if it exists.
    public void removeReferencingConstraint(SQLForeignKeyConstraint constraint) {
        this.referencingConstraints.remove(constraint);
    }

    // Removes a referenced constrain if it contains the parameter column.
    public void removeReferencedConstraint(SQLForeignKeyConstraint constraint) {
        this.referencedConstraints.remove(constraint);
    }

    // Removes a referencing constrain if it contains the parameter column.
    public void removeReferancingConstraintContainingColumn(SQLColumn column) {
        for (SQLForeignKeyConstraint constraint: this.referencingConstraints) {
            if (constraint.containsColumn(column)) {
                this.referencingConstraints.remove(constraint);
                break;
            }
        }
    }

    // Removes a referenced constrain if it contains the parameter column.
    public void removeReferancedConstraintContainingColumn(SQLColumn column) {
        for (SQLForeignKeyConstraint constraint: this.referencedConstraints) {
            if (constraint.containsColumn(column)) {
                this.referencedConstraints.remove(constraint);
                break;
            }
        }
    }

    // Adds a new column.
    public void addColumn(SQLColumn column) {
        this.columns.add(column);

        // Add the column to the Primary or Foreign keys of the table
        if (column.isPrimary()) {
            this.primaryKey.add(column);
        }
        else if (column.isForeign()) {
            this.foreignKeys.add(column);
        }
    }

    // Adds a new column without adding it as PK.
    public void addRawColumn(SQLColumn column) {
        this.columns.add(column);
    }

    // Adds a list of columns.
    public void addAllColumns(List<SQLColumn> columns) {
        this.columns.addAll(columns);
    }

    /** Returns the column with the given name, or null if not found. */ 
    public SQLColumn getColumnByName(String columnName) {
        for (SQLColumn column : this.columns) {
            if (column.getName().equals(columnName)) {
                return column;
            }
        }

        return null;
    }


    /**
     * @return A boolean indicating if the relation contains textual attributes.
     */
    public boolean containsTextualAttributes() {
        for (SQLColumn column: this.columns)
            if (column.getType().isTextual())
                return true;
        return false;
    }

    // Get number of ForeignKeys Contained in the PrimaryKey
    public int getNumOfFKsInPK() {
        int num = 0;
        // Both primaryKey and foreignKeys are sets of columns, so no duplicates exist.
        for (SQLColumn pkColumn: this.primaryKey) 
            for (SQLColumn fkColumn: this.foreignKeys)
                if (fkColumn.equals(pkColumn))
                    num++;
        return num;
    }

    public void debugPrint() {
        System.out.println(this.name);
        System.out.println("Columns :");
        for (SQLColumn column: this.columns)
            System.out.println("-- " + column);
        System.out.println("PrimaryKey :");
        for (SQLColumn column: this.primaryKey)
            System.out.println("-- " + column);
        System.out.println("ForeignKeys :");
        for (SQLColumn column: this.foreignKeys)
            System.out.println("-- " + column);
        System.out.println("Referencing Constraints :");
        for (SQLForeignKeyConstraint constraint: this.referencingConstraints)
            System.out.println("-- " + constraint);
        System.out.println("Referenced Constraints :");
        for (SQLForeignKeyConstraint constraint: this.referencedConstraints)
            System.out.println("-- " + constraint);
    }

    public String toAbbreviation() {
        return this.getName();
    }

    @Override
    public String toString () {
        return this.name;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 + hash + ((this.name == null) ? 0 : this.name.hashCode());
        return hash;
    }

    // Override equals() to compare two SQLTable objects.
    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof SQLTable)) return false;

        // Typecast o to SQLTable so that we can compare the data members.
        SQLTable c = (SQLTable) o;

        // Table names are unique inside a database schema.
        return this.name.equals(c.name);
    }

}
