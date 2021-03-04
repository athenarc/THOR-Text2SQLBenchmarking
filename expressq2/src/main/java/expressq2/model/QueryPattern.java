package expressq2.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import shared.util.Graph;

public class QueryPattern extends Graph<PatternNode, Graph.NoLabel>{
    
    private static final double PENALTY_MULTIPLIER = 3;
    
    // A Map storing a Keyword for each PatternNode that is non intermediate.
    // A non intermediate node is a node referring to an Entity that was searched
    // by the user, and its keyword is in the Users Query. All other nodes in the graph
    // are intermediate nodes and help connecting of the above nodes.    
    private HashMap<Keyword, PatternNode> keywordToPatternNode;

    // An Aggregate Annotation Created by the QueryInterpreter in the case that 
    // this QueryPattern represents a nested Query. In other cases this field is null.
    private OperationAnnotation nestedAggregateAnnotation;


    // This boolean indicates whether the Query represented by 
    // this QueryPattern is an aggregate Query or a Simple one.
    private boolean aggregateQuery;


    // Each TagGrouping used to Create this QueryPattern contains Tags that may or may not contain
    // a penalty. Increment the penalty points based on the Tags creating this QueryPattern. This 
    // penalty points will contribute in the scoring procedure and lower the score of a query pattern.
    private int penaltyPoints;

    /** 
     * Constructor 
     */
    public QueryPattern() {
        super();
        this.keywordToPatternNode = new HashMap<>();
        this.nestedAggregateAnnotation = null;
        this.penaltyPoints = 0;
        this.aggregateQuery = false;
    }
       
    /**
     * Create a QueryPattern clone containing only Vertexes of this QueryPattern.     
     * 
     * @return A clone of the Query Pattern.
     */
    public QueryPattern shallowClone() {
        QueryPattern clone = new QueryPattern();
        clone.keywordToPatternNode = new HashMap<>(this.keywordToPatternNode);
        clone.penaltyPoints = this.penaltyPoints;
        clone.aggregateQuery = this.aggregateQuery;
        clone.cloneVertexes(this);
        return clone;
    }

    /**
     * Create a QueryPattern clone containing only Vertexes of this QueryPattern.     
     * 
     * @param fillNodeToClonedNode A map between this Pattern's nodes and cloned Pattern's nodes. If its not null fill it.
     * @return A clone of the Query Pattern.
     */
    public QueryPattern deepClone(HashMap<PatternNode, PatternNode> fillNodeToClonedNode) {
        QueryPattern clone = new QueryPattern();                    // The Clone to return.
        clone.penaltyPoints = this.penaltyPoints;                   // Copy the penalty Points.
        clone.aggregateQuery = this.aggregateQuery;                 // Copy the boolean aggregateQuery.
        HashMap<PatternNode, PatternNode> nodeToClonedNode = null;  // A Map between nodes and cloned Nodes.

        // If the map is null create a local map.
        if (fillNodeToClonedNode == null)
            nodeToClonedNode = new HashMap<>();
        else 
            nodeToClonedNode = fillNodeToClonedNode;
            
        // Create a clone of this graph by cloning the super class first.
        clone.cloneLikeGraph(this, nodeToClonedNode); 
        
        // Now With the new nodes of cloned QueryPattern create the mapping 
        // between cloned PatterNodes and keywords for the clone. The mapping
        // will be constructed according tho this mapping.        
        for (Map.Entry<Keyword, PatternNode> entry: this.keywordToPatternNode.entrySet()) {
            PatternNode clonedNode = nodeToClonedNode.get(entry.getValue());  // The cloned Node matching with this entry's PatterNode.

            // Create an entry in the clone Pattern like the entry in this Patter.
            clone.keywordToPatternNode.put(entry.getKey(), clonedNode);
        }
        
        // Return the clone.
        return clone;
    }
    

    /**
     * Add a mapping between patterNode and Keyword.
     * 
     * @param patternNode The patterNode. 
     * @param keywords The List of keywords to map with the above patterNode.     
     */
    public void linkKeywordsWithPatterNode(List<Keyword> keywords, PatternNode patternNode) {
        for (Keyword keyword: keywords) {
            this.keywordToPatternNode.put(keyword, patternNode);
        }  
    }


    /** 
     * @param keyword the Keyword mapped with a patterNode in this QueryPatter.
     * @return The patterNode mapping with the above keyword.
     */
    public PatternNode getPatterNode(Keyword keyword) {
        return this.keywordToPatternNode.get(keyword);
    }

    /**          
     * @param relationName the name of th relation to search.
     * @return the PatterNode linked with the relation with name relationName
     */
    public PatternNode getPatterNode(String relationName) {
        for (PatternNode node: super.getVertexes())
            if (node.getReferredRelationName().equals(relationName))
                return node;
        return null;
    }
    
    
    /**          
     * @param annotation An Equality Annotation that the returned pattern Node must contain.
     * @return The PatternNode containing the parameter annotation.
     */
    public PatternNode getPatterNode(ConditionAnnotation annotation) {
        for (PatternNode node: super.getVertexes())
            if (node.containsAnnotation(annotation))
                return node;
        return null;
    }


    /**
     * Rank the QueryPattern based on a formula that computes a score for P by counting
     * the number of objects involved in the query pattern and the
     * average distance between the target and condition nodes.     
     *      
     * @param schemaGraph
     * @return the score of the QueryPattern. 
     */    
    public double getScore() {
        List<PatternNode> targetNodes = new ArrayList<>();          // Stores the pattern nodes that are Target Nodes.
        List<PatternNode> conditionNodes = new ArrayList<>();       // Stores the pattern nodes that are Condition Nodes.
        List<PatternNode> objectAndMixedNodes = new ArrayList<>();  // Stores the pattern nodes that correspond to Object And Mixed nodes.

        // Fill the above lists.
        this.fillTargetConditionMixedLists(targetNodes, conditionNodes, objectAndMixedNodes);

        // First calculate the average distance between the target and condition nodes.
        double avgDistance = 0.0;
        for (PatternNode targetNode: targetNodes) {
            for (PatternNode conditionNode: conditionNodes) {
                avgDistance += getDistanceBetweenNodes(targetNode, conditionNode);
            }            
        }
        
        if (avgDistance == 0.0) 
            avgDistance += 1.0;   // If the avg Distance is zero increment it by one so we wont return Nan.            
        else 
            avgDistance = avgDistance / (double) (targetNodes.size() * conditionNodes.size()) ;                

        // The penalty because of Keywords That have a better interpretation.
        double penalty = this.penaltyPoints * PENALTY_MULTIPLIER;
        
        // Then return 1 divided by the sum of #mixedNodes product avgDistance.
        return  1 / (((double) objectAndMixedNodes.size()) * avgDistance + penalty);
    }


    /**
     * Get 2 names of a relations in the SchemaGraph. Convert them to ORMNodes in the graph
     * and retrieve their distance in the graph. The distance is the total number of 
     * object and mixed nodes in the path connecting the two nodes.
     * 
     * @param nodeARelationName
     * @param nodeBRelationName
     * @return the distance of ORMNodes corresponding to parameters Relation Names.
     */
    public int getDistanceBetweenNodes(PatternNode nodeA, PatternNode nodeB) {        
        int distance = 0;

        // Get The Patch connecting those two nodes.        
        Graph<PatternNode, NoLabel> path = this.getPathConnecting2Nodes(nodeA, nodeB);
        // If not connected return a very big distance
        if (path == null) 
            return Integer.MAX_VALUE;

        for (PatternNode node: path.getVertexes())
            if (node.isObjectNode() || node.isMixedNode())
                distance++;
        
        // Return the distance.
        return distance;
    }

    /**
     * Fill the 3 ParameterLists with targetNodes, conditionNodes
     * and mixedNodes contained in the QueryPatter.
     * 
     * @param targetNodes
     * @param conditionNodes
     * @param mixedNodes     
     */
    private void fillTargetConditionMixedLists(
        List<PatternNode> targetNodes,
        List<PatternNode> conditionNodes,
        List<PatternNode> objectAndMixedNodes)
    {
        // Loop the patterNodes and decide if they are target condition or/and mixed nodes.
        for (PatternNode patternNode: super.getVertexes()) {
            if (patternNode.isTargetNode()) 
                targetNodes.add(patternNode);
            if (patternNode.isConditionNode())
                conditionNodes.add(patternNode);
            if (patternNode.isObjectOrMixedNode())
                objectAndMixedNodes.add(patternNode);            
        }

        // If no target nodes exist in the Graph, implicit insert one ,
        // using the radius of the graph.
        if (targetNodes.isEmpty()) {
            PatternNode tarNode = this.getRadius();
            targetNodes.add( tarNode );            
        }
    } 
    
    
    /**
     * @param penaltyPoints the penaltyPoints to set
     */
    public void setPenaltyPoints(int penaltyPoints) {
        this.penaltyPoints = penaltyPoints;
    }

    /**
     * @param nestedAggregateAnnotation the nestedAggregateAnnotation to set
     */
    public void setNestedAggregateAnnotation(OperationAnnotation nestedAggregateAnnotation) {
        this.nestedAggregateAnnotation = nestedAggregateAnnotation;
    }

    /**
     * @return the nestedAggregateAnnotation
     */
    public OperationAnnotation getNestedAggregateAnnotation() {
        return nestedAggregateAnnotation;
    }


    /**
     * @return a boolean indicating if the QueryPattern represents a nested Query.
     */
    public boolean isNested(){
        return this.nestedAggregateAnnotation != null;
    }

    /**
     * @return the aggregateQuery
     */
    public boolean isAggregateQuery() {
        return aggregateQuery;
    }

    /**
     * @param aggregateQuery the aggregateQuery to set
     */
    public void setAggregateQuery(boolean aggregateQuery) {
        this.aggregateQuery = aggregateQuery;
    }
}