package shared.database.model;

import shared.util.PrintingUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

// This class models a SQL tuple (row) of a relation.
public class SQLTuple {

    protected List<SQLColumn> attributes; // The attributes of the tuple.
    protected List<SQLValue> values;      // The values of the attributes.
    protected List<SQLColumn> primaryKey;  // List of attributes that compose the tuple's primary key.    

    public SQLTuple() {
        this.attributes = new ArrayList<SQLColumn>();
        this.values = new ArrayList<SQLValue>();
        this.primaryKey = new ArrayList<SQLColumn>();
    }

    // Getters and Setters.
    public List<SQLColumn> getAttributes() {
        return this.attributes;
    }

    public List<SQLValue> getValues() {
        return this.values;
    }

    // Adds a new attribute.
    public void addAttribute(SQLColumn attribute) {
        this.attributes.add(attribute);
    }

    // Adds a new value.
    public void addValue(SQLValue value) {
        this.values.add(value);
    }

    // Removes the last value.
    public void removeLastValue() {
        this.values.remove(this.values.size()-1);
    }

    // Returns true for systems that assign a score to their tuples.
    public boolean hasScoreField() {
        return false;
    }
    
    // Checks if the tuple contains the given keyword.
    public boolean containsKeyword(String keyword) {
        for (SQLValue value : this.values) {
            if (value.contains(keyword)) {
                return true;
            }
        }

        return false;
    }

    // Checks if the tuple contains all keywords from the given list.
    public boolean containsAllKeywords(Collection<String> keywords) {
        for (String keyword : keywords) {
            if (!this.containsKeyword(keyword)) {
                return false;
            }
        }

        return true;
    }


    // Get the value of the parameter column if it exists.
    public SQLValue getValueOfColumn(SQLColumn column) {
        for (int i = 0; i < this.attributes.size(); i++) {
            if (this.attributes.get(i).equals(column)) {
                return this.values.get(i);
            }
        }

        return null;
    }

    // Get the value of the parameter columnName if it exists.
    public SQLValue getValueOfColumnWithName(String columnName) {
        for(int i = 0; i < this.attributes.size(); i++) {
            if (this.attributes.get(i).getName().equals(columnName)) {
                return this.values.get(i);
            }
        }

        return null;
    }

    // Fills an SQLTuple object with the names and values of the given result set
    public void fill(SQLDatabase database, ResultSet rs) {
        try {
            ResultSetMetaData metadata = rs.getMetaData();    // The result set metadata                      

            // All indexes are +1 because it indexes in rs starts from 1

            // Loop the Results Set Columns
            for (int index = 0; index < metadata.getColumnCount(); index++) {
                SQLColumn column = null;
                
                // Get the column from the database.                
                SQLTable table = database.getTableByName(metadata.getTableName(index + 1));     // First get the Table
                if (table != null)                
                    column = table.getColumnByName(metadata.getColumnName(index + 1));  // Then get the Column

                // If we dint find the column this means we are returning an Aggregate function
                // so create a new column
                if (column == null) {                    
                    column = new SQLColumn(null, metadata.getColumnLabel(index + 1), SQLType.NUMERIC_TYPE, null);
                }
            
                // Add its value depending on the column type
                if (column.getType().isTextual()) {
                    this.values.add(new SQLVarcharValue(rs.getString(index+1), column.getType().getMaximumLength()));
                    this.attributes.add(column);
                }
                else if (column.getType().isInt()) {
                    this.values.add(new SQLIntValue(rs.getInt(index+1)));
                    this.attributes.add(column);
                }
                else if (column.getType().isDouble()) {
                    this.values.add(new SQLDoubleValue(rs.getDouble(index+1)));
                    this.attributes.add(column);
                }
                else if (column.getType().isFloat()) {
                    this.values.add(new SQLFloatValue(rs.getFloat(index+1)));
                    this.attributes.add(column);
                }

                // Check if the column is part of the tuple's primary key.
                if (column.isPrimary()) {
                    this.primaryKey.add(column);
                }
            }         

        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }


    // Fills an SQLTuple object with the names and values of the given list of columns.
    public void fill(List<SQLColumn> columns, ResultSet rs) {
        try {
            for (int index = 0; index < columns.size(); index++) {
                SQLColumn column = columns.get(index);
                if (column.getType().isTextual()) {
                    this.values.add(new SQLVarcharValue(rs.getString(index+1), column.getType().getMaximumLength()));
                    this.attributes.add(column);
                }
                else if (column.getType().isInt()) {
                    this.values.add(new SQLIntValue(rs.getInt(index+1)));
                    this.attributes.add(column);
                }
                else if (column.getType().isDouble()) {
                    this.values.add(new SQLDoubleValue(rs.getDouble(index+1)));
                    this.attributes.add(column);
                }
                else if (column.getType().isFloat()) {
                    this.values.add(new SQLFloatValue(rs.getFloat(index+1)));
                    this.attributes.add(column);
                }               

                // Check if the column is part of the tuple's primary key.
                if (column.isPrimary()) {
                    this.primaryKey.add(column);
                }
            }         
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int hashCode() {
        int hash = 17;
        int tupleHash = 7;
        for (SQLColumn column : this.primaryKey) {
            int index = this.attributes.indexOf(column);
            tupleHash += this.values.get(index).hashCode();
        }
        hash = 31 * tupleHash;

        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof SQLTuple)) return false;

        SQLTuple rhs = (SQLTuple) obj;

        // Compare the sizes of the primary keys.
        if (this.primaryKey.size() != rhs.primaryKey.size()) return false;

        // Compare the values of the primary key attributes.
        for (int thisIndex = 0; thisIndex < this.primaryKey.size(); thisIndex++) {
            // Get the index of the rhs column which is the same as this column.
            int rhsIndex = rhs.primaryKey.indexOf(this.primaryKey.get(thisIndex));

            // If the this primary key column is not contained in the rhs primary key columns return false.
            if (rhsIndex == -1) return false; 
            // Else compare the values and if not equal return false.
            else if (!this.values.get(thisIndex).equals(rhs.values.get(rhsIndex))) return false;
        }        
        return true;
    }


    public String simplePrint() {
        String str = new String();

        for (SQLValue value : this.values) {
            str += value + " ";
        }

        return str;
    }

    @Override
    public String toString() {
        String str = new String();
            
        // Initialize number of attributes.
        int attributesNumber = attributes.size();

        // An array storing the max length of Strings per column.
        // The Strings per column are tuple Values and attributes.
        int[] maxColumnLengthOfStrings = new int[attributesNumber];

        // Loop all the attributes and fill the max length array.
        for (int index = 0; index < attributesNumber; index++) {

            // Find the longer string between attribute and value.
            if (attributes.get(index).getName().length() > values.get(index).toString().length()) {
                maxColumnLengthOfStrings[index] = attributes.get(index).getName().length();
            }
            else {
                maxColumnLengthOfStrings[index] = values.get(index).toString().length();
            }
        }

        // The list of attributes to String format.
        String attributesList = new String();
        // A line used to separate the attributes from the data.
        String separationLine = new String();
        // Create the separation line and the attributes line.
        for (int index = 0; index < attributesNumber; index++) {
            attributesList += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                                                        this.attributes.get(index).getName(), " ");
            separationLine += "+" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index], "", "-");
        }
        attributesList += "|";  // Add the last "|".
        separationLine += "+"; // Add the last "+".

        // Print the attributes between separation lines.
        str += separationLine + "\n" + attributesList + "\n" + separationLine + "\n";

        // Print the values of the tuple.
        String rowOfValues = new String();
        for (int index = 0; index < attributesNumber; index++) {
            rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars(
                maxColumnLengthOfStrings[index],
                this.getValues().get(index).toString(),
                " "
            );
        }
        rowOfValues = rowOfValues.replace("\n", " ") + "|";
        str += rowOfValues + "\n" + separationLine;

        return str;
    }

}
