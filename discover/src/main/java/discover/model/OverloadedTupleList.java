package discover.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.util.PrintingUtils;

// This class models a List of OverloadedTuples all having the same attributes.
public class OverloadedTupleList {

    List<SQLColumn> attributes;    // The tuples attributes.
    List<OverloadedTuple> tuples;  // A list of tuples.
    String network; 

    public OverloadedTupleList(List<OverloadedTuple> tuples) {
        // Initialize the list from the attributes of the first tuple in the list.
        if (!tuples.isEmpty()) {
            this.attributes = new ArrayList<>(tuples.get(0).getAttributes());
        }
        else {
            this.attributes = new ArrayList<>();
        }

        this.tuples = tuples;
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

    // Sub-lists the result to the desired number if only 
    // there are more tuples.
    public void subList(int size){
        if (this.tuples.size() > size)
            this.tuples = this.tuples.subList(0, size);
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

    // Returns true if tuples is empty.
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    /**
     * @param network the network to set
     */
    public void setNetwork(String network) {
        this.network = network;
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
                                        tuple.getValues().get(index).toString(), " ");
            }
            rowOfValues += "|";
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);
    }
    

    // Prints the list of tuples in an SQL like format.
    @Override
    public String toString() {
        if (this.tuples == null || this.tuples.isEmpty()) return "i";  
        String retStr = new String();      

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
        retStr += this.network + "\n";
        retStr += separationLine + "\n";
        retStr += attributesList + "\n";
        retStr += separationLine + "\n";

        // Print all the rows of Tuple Values.
        for (OverloadedTuple tuple: this.tuples) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars( maxColumnLengthOfStrings[index],
                                        tuple.getValues().get(index).toString(), " ");
            }
            rowOfValues += "|";

            // Replace any \n with a space
            retStr += rowOfValues.replace('\n', ' ') + "\n";
        }

        // Print a separation line.
        retStr += separationLine + "\n";

        return retStr;
    }
}
