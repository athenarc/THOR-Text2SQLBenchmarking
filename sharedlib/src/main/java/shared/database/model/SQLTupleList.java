package shared.database .model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.util.PrintingUtils;

// This class models a List of SQLTuples all having the same attributes.
public class SQLTupleList {

    List<SQLColumn> attributes; // The tuples attributes.
    List<SQLTuple> tuples; // A list of tuples.

    public SQLTupleList(List<SQLTuple> tuples) {
        // Initialize the list from the attributes of the first tuple in the list.
        if (!tuples.isEmpty()) {
            this.attributes = new ArrayList<>(tuples.get(0).getAttributes());
        }
        else {
            this.attributes = new ArrayList<>();
        }
        this.tuples = tuples;        
    }
   
    // Getters and Setters.
    public List<SQLColumn> getAttributes() {
        return attributes;
    }

    public List<SQLTuple> getTupleList() {
        return tuples;
    }

    /**
     * SubList the list of tuples from startIdx (inclusive) to stopIdx (exclusive)
     * 
     * @param startIdx
     * @param stopIdx
     */
    public void subList(int startIdx, int stopIdx) {
        if (this.tuples != null && this.tuples.size() >= stopIdx)
            this.tuples = this.tuples.subList(startIdx, stopIdx);
    }

    // Removes list of tuples.
    public void removeAllTuples(List<SQLTuple> tuples) {
        this.tuples.removeAll(tuples);
    }

    // Returns true if tuples is empty.
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    // Prints the list of tuples in an SQL like format.
    public void print() {
        if (this.tuples == null || this.tuples.isEmpty()) return;
       
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
            if (!this.attributes.get(index).getName().equals("score"))
                tablesSet.add((this.attributes.get(index).getTable()) );


            attributesList += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                                                        this.attributes.get(index).getName(), " ");
            separationLine += "+" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index], "", "-");
        }
        attributesList += "|";  // Add the last "|".
        separationLine += "+"; // Add the last "+".

        // Print the tables which contain this tuples (HACK WAY).        
        // String tablesInString = new String ("Tables joined : ");
        // for (SQLTable table: tablesSet) {
        //     tablesInString += table.toAbbreviation() + " |><| ";
        // }
        // System.out.println(tablesInString.substring(0, tablesInString.length()-5));

        // Print the attributes between separation lines.
        System.out.println(separationLine);
        System.out.println(attributesList);
        System.out.println(separationLine);

        // Print all the rows of Tuple Values.
        for (SQLTuple tuple: this.tuples) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars( maxColumnLengthOfStrings[index],
                                        tuple.getValues().get(index).toString(), " ");
            }
            rowOfValues += "|";
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);
        
        // Print the number of results
        if (this.tuples.size() == 1)
            System.out.println("1 result");
        else 
            System.out.println(this.tuples.size() + " results");
    }

}
