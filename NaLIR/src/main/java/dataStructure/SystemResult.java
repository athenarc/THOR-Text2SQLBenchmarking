package dataStructure;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import shared.connectivity.thor.response.ColumnValuePair;
import shared.connectivity.thor.response.ResultInterface;

/**
 * Represents the System Result. Each result contains a single answer for the
 * users query.
 */
public class SystemResult implements ResultInterface {

    List<ColumnValuePair> result;  // A single result (tuple like format).
    Set<String> networks;    
    String query;
    
    /**
     * Empty Constructor
     */
    public SystemResult() {
        this.result = new ArrayList<>();
        this.query = null;
    }

    /**
     * Construct the System Result using a ResultSet.
     * 
     * @param rs A java ResultSet Object.
     * @param query The query used to produce this result.
     */
    public SystemResult(ResultSet rs, String query) {
        this.result = new ArrayList<>();
        this.query = query;

        try {
            ResultSetMetaData metadata = rs.getMetaData();    // The result set metadata

            // Loop the Results
            // Note : All indexes are +1 because it indexes in rs starts from 1
            for (int index = 0; index < metadata.getColumnCount(); index++) {
                this.result.add(new ColumnValuePair(
                    metadata.getColumnName(index + 1), 
                    (rs.getObject(index + 1) == null) ? (new String("NULL")) : (rs.getObject(index + 1)) )
                );
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public SystemResult(ResultSet rs, String query, Set<String> networks) {
        this.result = new ArrayList<>();
        this.query = query;
        this.networks = networks;

        try {
            ResultSetMetaData metadata = rs.getMetaData();    // The result set metadata

            // Loop the Results
            // Note : All indexes are +1 because it indexes in rs starts from 1
            for (int index = 0; index < metadata.getColumnCount(); index++) {
                this.result.add(new ColumnValuePair(
                    metadata.getColumnName(index + 1), 
                    (rs.getObject(index + 1) == null) ? (new String("NULL")) : (rs.getObject(index + 1)))
                );
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public Collection<ColumnValuePair> getColumnValuePairs() {
        return this.result;
    }

    @Override
    public Collection<String> getNetworks() {
        return this.networks;
    }

    @Override
    public String getQuery() {
        return this.query;
    }

    @Override
    public double getResultScore() {
        return 0;
    }

    @Override
    public boolean hasScoreField() {
        return false;
    }


    // Print all the results
    public static void print(List<SystemResult> results) {
        if (results == null || results.isEmpty()) return;
       
        // Initialize number of attributes.
        int attributesNumber = results.get(0).result.size();

        // An array storing the max length of Strings per column.
        // The Strings per column are tuple Values and attributes.
        int[] maxColumnLengthOfStrings = new int[attributesNumber];

        // Loop all the attributes and fill the max length array
        for (int index = 0; index < attributesNumber; index++) {

            // Initialize the array with the attributes name length.
            maxColumnLengthOfStrings[index] = results.get(0).result.get(index).getColumn().length();

            // Loop the values and find the longest value toString().
            for (int rowIndex = 0; rowIndex < results.size(); rowIndex++) { // Loop the rows
                String value = results.get(rowIndex).result.get(index).getValue().toString();
                if (value.length() > maxColumnLengthOfStrings[index]){
                    maxColumnLengthOfStrings[index] = value.length();
                }
            }
        }
        
        // The list of attributes to String format.
        String attributesList = new String();
        // A line used to separate the attributes from the data.
        String separationLine = new String();
        // Create the separation line and the attributes line.
        for (int index = 0; index < attributesNumber; index++) {

            attributesList += "|" + addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                      results.get(0).result.get(index).getColumn(), " ");
            separationLine += "+" + addStringWithLeadingChars(maxColumnLengthOfStrings[index], "", "-");
        }
        attributesList += "|";      // Add the last "|".
        separationLine += "+";      // Add the last "+".        

        // Print the attributes between separation lines.
        System.out.println(separationLine);
        System.out.println(attributesList);
        System.out.println(separationLine);

        // Print all the rows of Tuple Values.
        for (SystemResult tuple: results) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + addStringWithLeadingChars( maxColumnLengthOfStrings[index],
                                        tuple.result.get(index).getValue().toString(), " ");
            }
            rowOfValues += "|";
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);
        
        // Print the number of results
        if (results.size() == 1)
            System.out.println("1 result");
        else 
            System.out.println(results.size() + " results");    
    }



     // Returns a string containing the @param str leading with a number of @param characters.
    // The @param numOfChars is the number of characters that the returned string will have.
    // If the str's length is shorter that numOfChars then add characters to the string.
    // Adds also two spaces one in front and one at the end of the new string.
    public static String addStringWithLeadingChars(int numOfChars, String str, String character) {
        if (str.length() > numOfChars) return null;

        // The new string to return.
        String newString = new String();

        // The number of leading chars to add.
        int leadingSpaces = numOfChars - str.length();

        // First add the str and then add the chars.
        newString += " " +  str;
        for (int i = 0; i < leadingSpaces; i++) {
            newString += character;
        }        

        // Return the new String.
        return newString + " ";
    }
}