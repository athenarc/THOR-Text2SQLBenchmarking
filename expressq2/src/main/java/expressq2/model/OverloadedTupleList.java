package expressq2.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLDoubleValue;
import shared.database.model.SQLTable;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.util.PrintingUtils;

// This class models a List of OverloadedTuples all having the same attributes.
public class OverloadedTupleList {

    List<SQLColumn> attributes;    // The tuples attributes.
    List<OverloadedTuple> tuples;  // A list of tuples.    

    public OverloadedTupleList(List<OverloadedTuple> tuples) {
        // Initialize the list from the attributes of the first tuple in the list.
        if (!tuples.isEmpty()) {
            this.attributes = new ArrayList<>(tuples.get(0).getAttributes());
        }
        else {
            this.attributes = new ArrayList<>();
        }

        this.tuples = tuples;
        Collections.<OverloadedTuple>sort(this.tuples, new OverloadedTuple.ScoreComparator());
    }

    // Truncate TupleList by removing tuples that
    // dont contain all keywords in the @keywords List.
    public void truncate(List<String> keywords) {
        List<OverloadedTuple> newTupleList = new ArrayList<>();
        for(OverloadedTuple tuple: this.tuples)
            if (tuple.containsAllKeywords(keywords))
                newTupleList.add(tuple);
    
        this.tuples = newTupleList;
    }

    /**     
     * Keep only k tuples in the TupleList.
     * 
     * @param k The tuples to keep.
     */
    public void keepTopK(int k) {
        if (this.tuples.size() > k)
            this.tuples = this.tuples.subList(0, k);            
    }

    // Getters and Setters.
    public List<SQLColumn> getAttributes() {
        return attributes;
    }

    public List<OverloadedTuple> getTupleList() {
        return tuples;
    }

    // Removes list of tuples.
    public void removeAllTuples(List<OverloadedTuple> tuples) {
        this.tuples.removeAll(tuples);
    }

    // Prunes the Tuple list if more than param size and returns true. Else return false
    public boolean pruneTupleList(int size) {
        if (this.tuples.size() > size) {
            this.tuples = this.tuples.subList(0, size);
            return true;
        }
        return false;
    }

    // Returns true if tuples is empty.
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    // Returns the score of the last tuple in the list since
    // they are sorted in descending order according to their score.
    public Double getMinScore() {
        return this.tuples.get(tuples.size()-1).getScore();
    }

    // Prints the list of tuples in an SQL like format.
    public void print(Boolean printScore) {
        if (this.tuples == null || this.tuples.isEmpty()) return;

        // Add the score attribute.
        if (printScore) {
            SQLType columnType = new SQLType("double", 0);
            SQLColumn scoreColumn = new SQLColumn(null, "score", columnType, "");
            attributes.add(scoreColumn);

            // Add the value of the score column.
            for(OverloadedTuple tuple: this.tuples) {
                SQLValue scoreValue = new SQLDoubleValue(tuple.getScore());
                tuple.addValue(scoreValue);
            }
        }

        // Initialize number of attributes.
        int attributesNumber = attributes.size();

        // An array storing the max length of Strings per column.
        // The Strings per column are tuple Values and attributes.
        int[] maxColumnLengthOfStrings = new int[attributesNumber];

        // Loop all the attributes and fill the max length array
        for (int index = 0; index < attributesNumber; index++) {

            // Initialize the array with the attributes name length.
            maxColumnLengthOfStrings[index] = attributes.get(index).getName().length();

            // Loop the values and find the longest value toString().
            for (int rowIndex = 0; rowIndex < this.tuples.size(); rowIndex++) { // Loop the rows
                String value = this.tuples.get(rowIndex).getValues().get(index).toString();
                if (value.length() > maxColumnLengthOfStrings[index]){
                    maxColumnLengthOfStrings[index] = value.length();
                }
            }
        }

        // A set of tables whose columns are in the attributes list.
        Set<SQLTable> tablesSet = new HashSet<>();
        // The list of attributes to String format.
        String attributesList = new String();
        // A line used to separate the attributes from the data.
        String separationLine = new String();
        // Create the separation line and the attributes line.
        for (int index = 0; index < attributesNumber; index++) {

            // The score column has a null table. Dont search it.
            if (this.attributes.get(index).getTable() != null)
                tablesSet.add((this.attributes.get(index).getTable()) );


            attributesList += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                                                        this.attributes.get(index).getName(), " ");
            separationLine += "+" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index], "", "-");
        }
        attributesList += "|";  // Add the last "|".
        separationLine += "+"; // Add the last "+".

        // Print the tables which contain this tuples (HACK WAY).        
        String tablesInString = new String ("Tables joined : ");
        for (SQLTable table: tablesSet) {
            tablesInString += table.toAbbreviation() + " |><| ";
        }
        System.out.println(tablesInString.substring(0, tablesInString.length()-5));

        // Print the attributes between separation lines.
        System.out.println(separationLine);
        System.out.println(attributesList);
        System.out.println(separationLine);

        // Print all the rows of Tuple Values.
        for (OverloadedTuple tuple: this.tuples) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars( maxColumnLengthOfStrings[index],
                                        tuple.getValues().get(index).toString().replace("\n", " ") , " ");
            }
            rowOfValues += "|";
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);

        if (printScore) {
            // remove score from attribute list.
            this.attributes.remove(this.attributes.size() - 1);

            // Remove the value of the score column.
            for(OverloadedTuple tuple: this.tuples) {
                tuple.removeLastValue();
            }
        }
    }
    
}
