package shared.database.model;

import shared.util.Pair;

// This class models a SQL foreign key constraint between two columns.
public class SQLForeignKeyConstraint {

    private SQLColumn foreignKeyColumn; // The column with the foreign key.
    private SQLColumn primaryKeyColumn; // The referenced column.

    public SQLForeignKeyConstraint() {}

    public SQLForeignKeyConstraint(SQLColumn referencingColumn, SQLColumn referencedColumn) {
        this.foreignKeyColumn = referencingColumn;
        this.primaryKeyColumn = referencedColumn;
    }
   
    // Returns whether an SQLColumn is involved in the constrain
    // as a referenced or a referee column.
    public boolean containsColumn(SQLColumn column) {
        return column.equals(this.foreignKeyColumn) || column.equals(this.primaryKeyColumn);
    }

    // Returns whether an SQLTable is involved in the constrain.
    // If the reference or referee columns in this constrain is
    // a column of this SQLTable then we return true.
    public boolean containsTablesColumn(SQLTable table) {
        return table.getName().equals(this.foreignKeyColumn.getTableName()) ||
               table.getName().equals(this.primaryKeyColumn.getTableName());
    }

    // Returns the column of the SQLTable given as parameter
    // if it is a part of this constrain. Else return null.
    public Pair<SQLColumn, Boolean> getTablesColumn(SQLTable table) {
        if (table.getName().equals(this.foreignKeyColumn.getTableName())){ 
            return new Pair<SQLColumn,Boolean>(this.foreignKeyColumn, true);
        }
        else if (table.getName().equals(this.primaryKeyColumn.getTableName())) {
            return new Pair<SQLColumn,Boolean>(this.primaryKeyColumn, false);
        }
        else  return null;
    }



    // Returns a pair of SQLColumns where the @pair.left is the Column
    // that participates in the PK-FK Constrain for the @leftTable
    // and the @pair.right is the Column of the @rightTable.
    // If an PK-FK Constrain exists between those two tables, else 
    // return null.
    // Also sets to true the left or right term of the Boolean Pair 
    // for the referencing table.
    public Pair<SQLColumn, SQLColumn> getColumnPairIfTablesPatricipateInConstraint(
        SQLTable leftTable,
        SQLTable rightTable,
        Pair<Boolean, Boolean> foreignKeyTable)
    {
        Pair<SQLColumn, SQLColumn> pair = null;

        // There are no self joins so the constrains contain columns 
        // from different Tables. Those two columns cant be the same.
        Pair<SQLColumn, Boolean> leftColumnPair = this.getTablesColumn(leftTable);
        Pair<SQLColumn, Boolean> rightColumnPair = this.getTablesColumn(rightTable);        

        // If the Constrain contained columns from both Table create a Pair.
        if (leftColumnPair != null && rightColumnPair != null && leftColumnPair.getLeft() != rightColumnPair.getLeft()) {
            pair = new Pair<SQLColumn, SQLColumn>(leftColumnPair.getLeft(), rightColumnPair.getLeft());
            if (foreignKeyTable != null) {
                foreignKeyTable.setLeft(leftColumnPair.getRight());
                foreignKeyTable.setRight(rightColumnPair.getRight());
            }
        }        
    
        // Return the a new pair or a null pair.
        return pair;
    }


    // Fills the fields of an empty SQLForeignKeyConstraint.
    // Receives the names of the table and the column with the foreign key,
    // and the names of the table and the column with the primary key (referenced).
    // Throws an exception if the parameters are not valid.
    public void fill(SQLDatabase database, String tableName, String columnName,
                String referencedTableName, String referencedColumnName) {
        SQLTable table = database.getTableByName(tableName);
        this.setForeignKeyColumn(table.getColumnByName(columnName));

        SQLTable referencedTable = database.getTableByName(referencedTableName);
        this.setPrimaryKeyColumn(referencedTable.getColumnByName(referencedColumnName));
    }


    @Override
    public String toString() {
        return this.primaryKeyColumn.getTableName() + "." + this.primaryKeyColumn.getName() + "->" +
               this.foreignKeyColumn.getTableName() + "." + this.foreignKeyColumn.getName();
    }


     // Getters and Setters.
     public SQLColumn getForeignKeyColumn() {
        return this.foreignKeyColumn;
    }

    public void setForeignKeyColumn(SQLColumn referencingColumn) {
        this.foreignKeyColumn = referencingColumn;
    }

    public SQLColumn getPrimaryKeyColumn() {
        return this.primaryKeyColumn;
    }

    public void setPrimaryKeyColumn(SQLColumn referencedColumn) {
        this.primaryKeyColumn = referencedColumn;
    }
}
