package discover.components;

import java.util.List;
import java.util.ArrayList;

// The Parser receives a string (the input query)
// and tokenizes that string based on whitespace.
//
// Input: Query in string format
// Output: List of keywords
public class Parser {
    
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
    
}
