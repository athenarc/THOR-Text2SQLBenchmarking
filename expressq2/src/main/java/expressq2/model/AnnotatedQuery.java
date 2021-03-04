package expressq2.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import expressq2.model.Keyword.TermType;


/** 
 * This Class Models an AnnotatedQuery by the QueryAnalyzer.
 * It keeps the keywords in the order of the user Query along 
 * with the TagGroupings assigned to each keyword by the Analyzer.
*/
public class AnnotatedQuery {

    private List<Keyword> keywords;                   // The keywords of the Query.
    private Set<TagGrouping> tagGroupings;            // The tagGroupings produced by the above Keywords.        
    private HashMap<Keyword, Tag> keywordToTagMap;    // A mapping assigning a Tag to each keywords in this annotated query.   
    private int penaltyPoints;                        // For each contained Tag with a Penalty increment this integer.
    private boolean aggregateQuery;                   // Indicates if the Query contains any aggregate or GroupBy functions.

    // Helpful Lists.
    private List<Integer> operatorKeywordsIndices;
    private List<Integer> valueReferredKeywordsIndices;
    private List<Integer> componentRelationReferredKeywordsIndices;

    /** 
     * Public Constructor 
     * 
     * @param keywords The User's query keywords.
     */
    public AnnotatedQuery(List<Keyword> keywords) {
        this.keywords = new ArrayList<>(keywords);
        this.tagGroupings = new HashSet<>();    
        this.keywordToTagMap = new HashMap<>();
        this.penaltyPoints = 0;
        
        // Store the operator keywords indices in a separate list.
        this.operatorKeywordsIndices = new ArrayList<>();
        for (int index = 0; index < this.keywords.size(); index++)
            if (this.keywords.get(index).getType() == TermType.OperatorTerm)
                this.operatorKeywordsIndices.add(index);

        // Initialize the other helpful lists.
        this.valueReferredKeywordsIndices = new ArrayList<>();
        this.componentRelationReferredKeywordsIndices = new ArrayList<>();        

        // If the Query does not contain operator Keyword Indices then it's not an aggregate Query.
        this.aggregateQuery = (operatorKeywordsIndices.isEmpty()) ? (false) : (true);
    }
   
    /**
     * Adds a TagGrouping in the TagGroupings set.
     * 
     * @param tagGrouping the TagGrouping to insert.
     * @return if the insertions where successfully.     
     */
    public boolean addTagGrouping(TagGrouping tagGrouping) {
        return this.tagGroupings.add(tagGrouping);               
    }    

    /**
     * Updates the mapping of Keywords to Tags.     
     * 
     * @param keyword The keyword used to update the mapping.
     * @param tag The Tag mapping to the above keyword.     
     */
    public void linkKeywordWithTag(Keyword keyword, Tag tag) {
        // If the keyword is not contained in the Query then dont add a mapping.
        if (this.keywords.indexOf(keyword) == -1) return;

        // Else link the Keyword To the Tag.
        this.keywordToTagMap.put(keyword, tag);
    }


     /**
     * Extract information for the parameter keyword from the parameter Tag.
     * If a keyword refers to a component Relation or to a value in the 
     * database , update the corresponding list in this annotated Query.
     * 
     * @param keyword The keyword to examine.
     * @param tag The tag giving us information about the keyword.
     */
    public void extractKeywordsUse(Keyword keyword, Tag tag) {
        if (tag.refersToComponentRelation())
            this.componentRelationReferredKeywordsIndices.add(
                this.keywords.indexOf(keyword)
            );
        if (tag.getCond() != null)
            this.valueReferredKeywordsIndices.add(
                this.keywords.indexOf(keyword)
            );

        // Also link the Keyword with the tag and update this penaltyPoints.
        this.linkKeywordWithTag(keyword, tag);
        this.getPenaltyPoints(tag);
    }

    /**
     * If the Tag added to this Annotated Query has a penalty the increment the penalty Points
     * 
     * @param tag The tag inserted to this Annotated Query.
     */
    private void getPenaltyPoints(Tag tag) {
        if (tag.hasPenalty())
            this.penaltyPoints++;
    }

    /**
     * Simply print the queries keywords.
     */
    public String toSimpleString() {
        return this.keywords.toString();
    }    

    /**
     * Print the annotatedQuery by printing the TagGroupings
     */
    @Override
    public String toString() {
        return this.tagGroupings.toString();
    }


    /**
     * @return the keywords
     */
    public List<Keyword> getKeywords() {
        return keywords;
    }


    /**
     * @return the tagGroupings
     */
    public Set<TagGrouping> getTagGroupings() {
        return tagGroupings;
    }

    /**
     * @return the Penalty Points this Annotated Query Holds.
     */
    public int getPenaltyPoints() {
        return this.penaltyPoints;
    }

    /**
     * @return the operatorKeywordsIndices
     */
    public List<Integer> getOperatorKeywordsIndices() {
        return operatorKeywordsIndices;
    }

    /**
     * @return the componentRelationReferredKeywordsIndices
     */
    public List<Integer> getComponentRelationReferredKeywordsIndices() {
        return componentRelationReferredKeywordsIndices;
    }


    /**
     * @return the valueReferredKeywordsIndices
     */
    public List<Integer> getValueReferredKeywordsIndices() {
        return valueReferredKeywordsIndices;
    }
  
    /**     
     * @param keyword the keyword whose Tag we will return.
     * @return The tag of the above Keyword.
     */
    public Tag getKeywordsTag(Keyword keyword) {
        return this.keywordToTagMap.get(keyword);
    }

    /**
     * @return the aggregateQuery
     */
    public boolean isAggregateQuery() {
        return aggregateQuery;
    }

}