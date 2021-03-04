package expressq2.components;

import java.util.List;

import expressq2.model.AnnotatedQuery;
import expressq2.model.Keyword;
import expressq2.model.Tag;

import java.util.ArrayList;

// The Parser receives a string (the input query)
// and tokenizes that string based on whitespace.
//
// Input: Query in string format
// Output: List of keywords
public class Parser {

    // private static final Logger LOGGER = Logger.getLogger(QueryAnalyzer.class.getName());

    // Tokenizes the input string based on whitespace.
    public static List<Keyword> whitespaceTokenizer(String query) {
        // The list of keywords to be returned. Initially empty.
        List<Keyword> keywords = new ArrayList<>();

        // Loop all the strings chars
        int beginIndex = 0;
        boolean quoteOpened = false;
        for(int index = 0; index < query.length();) {
            // Get the char of the index position.
            char c = query.charAt(index);
            
            // Depending on the char handle the subStrings.
            if (c == ' ' && !quoteOpened) {
                keywords.add(new Keyword(query.substring(beginIndex, index))); // Create the subString till this index.
                index = findNextChar(query, index);               // Skip all the white chars for index.
                beginIndex = index;                               // Initialize the beginIndex for next subString.                
            }
            else if (c == '\"' && quoteOpened) {
                keywords.add(new Keyword(query.substring(beginIndex, index))); // Create the subString till this index.
                index = findNextChar(query, index + 1);           // Skip all the white chars for index.
                beginIndex = index;                               // Initialize the beginIndex for next subString.
                quoteOpened = false;                              // The quote closed.
            }
            else if (c == '\"' && !quoteOpened) {
                beginIndex = ++index;  // Initialize the beginIndex for next quoted subString. 
                quoteOpened = true;    // Note that a quote Opened.
            }
            else if (index == query.length() - 1) {                
                keywords.add(new Keyword(query.substring(beginIndex)));  // Create the final subString.
                break;
            } 
            else {
                index++;  // If c is not a special char, just increment the index.
            }
        }

        return keywords;
    }

    /**
     * Check the validity of an annotated query based on 3 rules:
     *  1. The last keyword cannot be an OperatorTerm
     *  2. For each term matching an AggregateFunction the next 
     *     should match an attribute name or an Aggregate Function.
     *  3. For each term matching an GroupBy or Count the next 
     *     should match a relation name or an attribute name.
     * Also report log a waring about the Queries that failed. 
     * 
     * @param annotatedQuery
     * @return
     */
    public static boolean checkAnnotatedQueryValidity(AnnotatedQuery annotatedQuery) {
        // Loop all the queries keywords.
        for (int index = 0; index < annotatedQuery.getKeywords().size(); index++) {
            Keyword keyword = annotatedQuery.getKeywords().get(index);
                        
            // Check the first rule
            if (index == annotatedQuery.getKeywords().size() - 1 && keyword.getType() == Keyword.TermType.OperatorTerm) {
                // LOGGER.warning(
                    // "Query: " + annotatedQuery.toSimpleString() +
                    // "\n[Non Valid] Has '" + keyword + "' operatorTerm as last term." +
                    // "\n[Info] Operator Terms must be followed by Relation/Attribute names.\n"
                // );
                return false;
            }
            // Check second rule.            
            else if (keyword.isAggregateFunctionExceptCOUNT()) {
                Keyword nextKeyword = annotatedQuery.getKeywords().get(index + 1);  // The next keyword.
                Tag tag = annotatedQuery.getKeywordsTag(nextKeyword);               // Get the Keyword's Tag.

                // Check if the nextKeyword refers to an Attribute Name or an aggregate Term.
                if (! ((tag != null && tag.refersToAttribute()) || nextKeyword.isAggregateFunction()) ) {
                    // LOGGER.warning(
                        // "Query: " + annotatedQuery.toSimpleString() +
                        // "\n[Non Valid] Has non Attribute Name/Aggregate Term '" + nextKeyword + "' after Aggregate Term '" + keyword + "'." +
                        // "\n[Info] Aggregation Terms must be followed by Attribute names/Aggregate Terms.\n"
                    // );
                    return false;
                }
            }
            // Check the third rule.
            else if (keyword.isGroupByFunction() || keyword.isCountFunction()) {
                Keyword nextKeyword = annotatedQuery.getKeywords().get(index + 1);  // The next keyword.
                Tag tag = annotatedQuery.getKeywordsTag(nextKeyword);               // Get the Keyword's Tag.

                // Check if the nextKeyword refers to an Attribute Name or a Relation Name.
                if (!(tag.refersToAttribute() || tag.refersToRelation())) {                    
                    // LOGGER.warning(
                        // "Query: " + annotatedQuery.toSimpleString() +
                        // "\n[Non Valid] Has non Relation/Attribute Name '" + nextKeyword + "' after Term '" + keyword + "'." +
                        // "\n[Info] GroupBy Terms must be followed by Relation/Attribute names.\n"
                    // );
                    return false;
                } 
                // And for the CountTerm only that it is a aggregateTerm check for a nested aggregate term.
                else if (keyword.isCountFunction() &&
                        ( ! (tag != null && ((tag.refersToAttribute() || tag.refersToRelation())) 
                            ||
                            nextKeyword.isAggregateFunction())))
                {
                    // LOGGER.warning(
                        // "Query: " + annotatedQuery.toSimpleString() +
                        // "\n[Non Valid] Has non Relation/Attribute Name/Aggregation Term '" + nextKeyword + "' after Term '" + keyword + "'." +
                        // "\n[Info] Count Terms must be followed by Relation/Attribute names/Aggregation Terms.\n"
                    // );
                    return false;
                }
            }
        }
        
        // If all 3 rules apply to all Keywords then return true.
        return true;
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
