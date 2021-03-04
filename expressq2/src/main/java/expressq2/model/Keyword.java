package expressq2.model;

import java.util.Arrays;
import java.util.List;

/** 
 * This Class models a Keyword in the User's query.
 */ 
public class Keyword {

    // The OperatorTerms.
    public static final String   GROUPBY_TERM = "GROUPBY";
    public static final String   COUNT_TERM = "COUNT";
    public static final List<String> AGGREGATE_TERMS = Arrays.asList("MIN", "MAX", "AVG", "SUM", "COUNT");
            

    // The Term Type
    public enum TermType { BasicTerm, OperatorTerm }
    
    String term;     // The actual phrase in the query.
    TermType type;   // What type is the phrase.    


    /**
     * Keyword Constructor. Inputs the Term of the keyword and 
     * determines the TermType : BasicTerm or OperatorTerm
     * 
     * @param keywordPhrase
     */
    public Keyword(String keywordTerm) {
        this.term = keywordTerm;

        // If this.term equals any Aggregate or GroupBy Function then its an operatorTerm.
        if (this.isOperatorTerm()) {
            this.type = TermType.OperatorTerm;
            this.term = term.toUpperCase();  
        } else {
            this.type = TermType.BasicTerm;
        }
    }

    /**
     * @return the term
     */
    public String getTerm() {
        return term;
    }

    /**
     * @return the type
     */
    public TermType getType() {
        return type;
    }

    /**        
     * @return A boolean indicating whether this.term is an Operator Term.
     * An operator term is :
     *  - A GroupBy Function (GROUPBY)
     *  - An Aggregate Function (MIN, MAX, AVG, SUM, COUNT)
     */
    public boolean isOperatorTerm() {
        return this.isGroupByFunction() || this.isAggregateFunction();
    }

    /**
     * @return True in case that the Keywords Term is an AggregateTerm.
     */
    public boolean isAggregateFunction() {
        return AGGREGATE_TERMS.indexOf(this.term.toUpperCase()) != -1;
    }

    /**
     * @return True in case that the Keywords Term is an AggregateTerm except Count.
     */
    public boolean isAggregateFunctionExceptCOUNT() {
        return !this.isCountFunction() && this.isAggregateFunction();
    }

    /**
     * @return True in case that the Keywords Term is an GroupByTerm.
     */
    public boolean isGroupByFunction() {
        return this.term.toUpperCase().equals(GROUPBY_TERM);
    }

    /**
     * @return True in case that the Keywords Term is an CountTerm.
     */
    public boolean isCountFunction() {
        return this.term.toUpperCase().equals(COUNT_TERM);
    }
    

    /**
     * Print the Keyword.
     */
    @Override
    public String toString() {
        return this.term;
    }

}
