package discoverIR.model;

import shared.connectivity.thor.response.ColumnValuePair;
import shared.connectivity.thor.response.ResultInterface;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLDoubleValue;
import shared.database.model.SQLFloatValue;
import shared.database.model.SQLIntValue;
import shared.database.model.SQLQuery;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.database.model.SQLVarcharValue;
import shared.util.PrintingUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

// This class models a SQL tuple (row) of a relation.
public class OverloadedTuple extends SQLTuple implements ResultInterface{

    public static class ScoreComparator implements Comparator<OverloadedTuple> {
        @Override
        public int compare(OverloadedTuple a, OverloadedTuple b) {
            return b.getScore().compareTo(a.getScore());
        }
    }
            
    private Map<String, Integer> keywordFrequencies;     // Maps a keyword with the number of times it appears in the tuple.
    private Double score;          // THe score of each tuple
    SQLQuery query;                // The sqlQuery used to get this tuple

    public OverloadedTuple() {
        super();
        this.score = 0.0;
        this.keywordFrequencies = new HashMap<String, Integer>();
    }

    // Getters and Setters.    
    public Double getScore() {
        return this.score;
    }

    public Map<String, Integer> getKeywordFrequencies() {
        return this.keywordFrequencies;
    }

    // Get the frequency of a keyword.
    public Integer getKeywordFrequency(String keyword) {
        return this.keywordFrequencies.get(keyword);
    }

    public Set<String> getKeywords() {
        return this.keywordFrequencies.keySet();
    }
        
    // Increments the score by the given value.
    public void incrementScore(Double inc) {
        this.score += inc;
    }
    
    public List<SQLColumn> getPrimaryKeys() {
        return super.primaryKey;
}

    /**
     * @param query the query to set
     */
    public void setQuery(SQLQuery query) {
        this.query = query;
    }

    // Increments the frequency of a keyword by 1.
    public void incrementKeywordFrequency(String keyword) {
        this.keywordFrequencies.put(keyword, this.keywordFrequencies.getOrDefault(keyword, 0) + 1);
    }

    // Given a query and an attribute this function checks if any keyword
    // from the given list is contained in the attribute's corresponding value.
    public void updateKeywordFrequencies(List<String> keywords, SQLColumn attribute) {
        // Get the value of the given attribute.
        int index = super.attributes.indexOf(attribute);
        if (index == -1) {
            System.err.println("[ERR] SQLTuple.updateKeywordFrequencies()");
            System.err.println("Attribute: " + attribute + " is not part of tuple's attributes: " + this.attributes);
            return;
        }

        SQLValue value = super.values.get(index);

        // Check if the value contains any keywords from the list.
        for (String keyword : keywords) {
            if (value.contains(keyword)) {
                this.keywordFrequencies.put(keyword, this.keywordFrequencies.getOrDefault(keyword, 0) + 1);
            }
        }
    }


    // Fills an SQLTuple object with the names and values of the given list of columns.
    public void fill(List<SQLColumn> columns, ResultSet rs) {
        try {
            // All indexes are +1 because the indexes in ResultSet starts from 1
            ResultSetMetaData metadata = rs.getMetaData();

            // Loop the Results Set Columns
            for (int index = 0; index < metadata.getColumnCount(); index++) {

                // Loop the columns and find the correct one
                SQLColumn column = null;
                for (SQLColumn col: columns)
                    if (col.getName().equals(metadata.getColumnName(index + 1)))
                        column = col;

                // If no column was found and ResultSet's column name is score then store the score.
                if (metadata.getColumnName(index + 1).equals("score"))
                    this.score = rs.getDouble(index+1);                                  
                // Otherwise create the correct Column
                else {
                    if (column.getType().isTextual()) {
                        this.addAttribute(column);
                        this.addValue(new SQLVarcharValue(rs.getString(index+1), column.getType().getMaximumLength()));
                    }
                    else if (column.getType().isInt()) {
                        this.addAttribute(column);
                        this.addValue(new SQLIntValue(rs.getInt(index+1)));
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
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Fills an SQLTuple object with the names and values of the given list of columns.
    // Also calculates the score attribute by dividing the sum of scores that sql 
    // gives with the size of the tree.
    public void fill(List<SQLColumn> columns, ResultSet rs, int size) {
        try {
            // All indexes are +1 because the indexes in ResultSet starts from 1
            ResultSetMetaData metadata = rs.getMetaData();

            // Loop the Results Set Columns
            for (int index = 0; index < metadata.getColumnCount(); index++) {

                // Loop the columns and find the correct one
                SQLColumn column = null;
                for (SQLColumn col: columns)
                    if (col.getName().equals(metadata.getColumnName(index + 1)))
                        column = col;

                // If no column was found and ResultSet's column name is score then store the score.
                if (metadata.getColumnName(index + 1).equals("score"))
                    this.score = rs.getDouble(index+1) / size;                                  
                // Otherwise create the correct Column
                else {
                    if (column.getType().isTextual()) {
                        this.addAttribute(column);
                        this.addValue(new SQLVarcharValue(rs.getString(index+1), column.getType().getMaximumLength()));
                    }
                    else if (column.getType().isInt()) {
                        this.addAttribute(column);
                        this.addValue(new SQLIntValue(rs.getInt(index+1)));
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
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
    

    @Override
    public String simplePrint() {
        String str = new String();

        for (SQLValue value : super.values) {
            str += value + " ";
        }
        str += "(" + this.score + ")";

        return str;
    }

    @Override
    public String toString() {
        String str = new String();

        // Create a score column.
        SQLType columnType = new SQLType("double", 0);
        SQLColumn scoreColumn = new SQLColumn(null, "score", columnType, "");
        attributes.add(scoreColumn);

        // Add the value of the score column.
        SQLValue scoreValue = new SQLDoubleValue(this.getScore());
        this.addValue(scoreValue);

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
        rowOfValues += "|";
        str += rowOfValues + "\n" + separationLine;

        // remove score from attribute list.
        this.attributes.remove(this.attributes.size() - 1);

        // Remove the value of the score column.
        this.removeLastValue();

        return str;
    }


    /***********************************
     * Interface Implemented Functions *
     ***********************************/

	@Override
	public Collection<ColumnValuePair> getColumnValuePairs() {		
        List<ColumnValuePair> cvPairs = new ArrayList<ColumnValuePair>();

        for (int i= 0; i < this.getAttributes().size() ; i++ ) {
            cvPairs.add(new ColumnValuePair(this.attributes.get(i).getName(), 
                this.values.get(i).getValue())
            );
        }
        return cvPairs;
	}

	@Override
	public Collection<String> getNetworks() {
		return this.query.getTables();
	}

	@Override
	public String getQuery() {
		return this.query.toPrettyQuery();
    }
    
    @Override
    public boolean hasScoreField() {
        return true;
    }  

	@Override
	public double getResultScore() {
		return this.score;
	}

}
