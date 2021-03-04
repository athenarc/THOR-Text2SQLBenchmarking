package expressq2.model;

import shared.connectivity.thor.response.ColumnValuePair;
import shared.connectivity.thor.response.ResultInterface;
import shared.database.model.SQLColumn;
import shared.database.model.SQLDoubleValue;
import shared.database.model.SQLTuple;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.util.PrintingUtils;

import expressq2.model.SQLQuery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

// This class models a SQL tuple (row) of a relation.
public class OverloadedTuple extends SQLTuple implements ResultInterface {

    public static class ScoreComparator implements Comparator<OverloadedTuple> {
        @Override
        public int compare(OverloadedTuple a, OverloadedTuple b) {
            return b.getScore().compareTo(a.getScore());
        }
    }
                
    private Double score;          // THe score of each tuple
    SQLQuery query;                // The sqlQuery used to get this tuple

    public OverloadedTuple() {
        super();
        this.score = 0.0;
        this.query = null;
    }

    // Getters and Setters.    
    public Double getScore() {
        return this.score;
    }    

    public void setScore(Double score) {
        this.score = score;
    }
   
    /**
     * @param query the query to set
     */
    public void setQuery(SQLQuery query) {
        this.query = query;
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
        for (SQLColumn column : this.getAttributes()) {
            cvPairs.add(new ColumnValuePair(column.getName(), this.getValueOfColumn(column).getValue()));
        }
        return cvPairs;
	}

	@Override
	public Collection<String> getNetworks() {
		return this.query.getTables();
	}

	@Override
	public String getQuery() {
		return this.query.getQueryToString();
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
