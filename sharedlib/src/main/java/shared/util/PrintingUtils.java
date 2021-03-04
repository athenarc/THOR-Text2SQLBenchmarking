package shared.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

// A class with static functions. The functions
// do procedures helpful in printing.
public class PrintingUtils {


    /**
     * Escape char ch by adding the escpaeCH in front of it
     * 
     * @param str
     * @param ch
     * @param escapeCh
     * @return
     */
    public static String escapeCharacter(String str, char ch, char escapeCh) {        
        int indexOfChar = str.indexOf(ch);
        while(indexOfChar != -1) {
            str = str.substring(0, indexOfChar) + escapeCh + str.substring(indexOfChar, str.length());


            // Update index of character.
            indexOfChar = str.indexOf(ch, indexOfChar + 2); // Pass the ch and escapeCh (with the + 2).
        }
        
        return str;
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

    public static String addPrefixCharNumTimesToStr(int numOfChars, String str, String character) {
        // The new string to return.
        String newString = new String();

        // The number of leading chars to add.
        int leadingChars = numOfChars;

        // First add the chars and then add the string
        for (int i = 0; i < leadingChars; i++) {
            newString += character;
        }
        newString += str;

        // Return the new String.
        return newString;
    }

    public static String addPostfixCharNumTimesToStr(int numOfChars, String str, String character) {
        // The new string to return.
        String newString = new String();

        // The number of chars to add.
        int chars = numOfChars;

        // First add the str and then add the chars.        
        newString += str;
        for (int i = 0; i < chars; i++) {
            newString += character;
        }        

        // Return the new String.
        return newString;
    }


    public static String surroundStringWithDelimiter(String str, String delimiter) {        
        return PrintingUtils.addPostfixCharNumTimesToStr(
            str.length(),
            PrintingUtils.addPrefixCharNumTimesToStr(str.length(),"\n" + str + "\n" , delimiter),
            delimiter);
    }
    

    /**
     * Generic Printing function.
     */
    public static <T> String separateWithDelimiter(Collection<T> collection, String delimiter) {
        String retStr = new String();

        // Loop all the strings and add the delimiter in the end of each one
        for (T col: collection)
            retStr += col.toString() + delimiter;
        
        // Remove the last delimiter            
        if (!retStr.isEmpty())
            retStr = retStr.substring(0, retStr.length() - delimiter.length());

        return retStr;
    }


    // Finds the next char , skipping all the white space characters , starting from parameter index.
    public static int findNextChar(String query, int index) {
        int returnIndex = index;
        // Loop till we get a non White Space char.
        while (returnIndex < query.length() && isWhiteSpace(query.charAt(returnIndex)))
            returnIndex++;
        // Return the index
        return returnIndex;
    }

    // Returns true if char is a white space char.
    public static boolean isWhiteSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n';
    }


    // Tokenizes the input string based on whitespace.
    public static List<String> whitespaceTokenizer(String query) {
        // The list of Strings to be returned. Initially empty.
        List<String> Strings = new ArrayList<>();

        // Loop all the strings chars
        int beginIndex = 0;
        boolean quoteOpened = false;
        for(int index = 0; index < query.length();) {
            // Get the char of the index position.
            char c = query.charAt(index);
            
            // Depending on the char handle the subStrings.
            if (c == ' ' && !quoteOpened) {
                Strings.add(new String(query.substring(beginIndex, index))); // Create the subString till this index.
                index = findNextChar(query, index);               // Skip all the white chars for index.
                beginIndex = index;                               // Initialize the beginIndex for next subString.                
            }
            else if (c == '\"' && quoteOpened) {
                Strings.add(new String(query.substring(beginIndex, index))); // Create the subString till this index.
                index = findNextChar(query, index + 1);           // Skip all the white chars for index.
                beginIndex = index;                               // Initialize the beginIndex for next subString.
                quoteOpened = false;                              // The quote closed.
            }
            else if (c == '\"' && !quoteOpened) {
                beginIndex = ++index;  // Initialize the beginIndex for next quoted subString. 
                quoteOpened = true;    // Note that a quote Opened.
            }
            else if (index == query.length() - 1) {                
                Strings.add(new String(query.substring(beginIndex)));  // Create the final subString.
                break;
            } 
            else {
                index++;  // If c is not a special char, just increment the index.
            }
        }

        return Strings;
    }
    
}