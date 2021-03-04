package expressq2.model;

import java.util.HashSet;
import java.util.Set;

import shared.database.model.graph.ORMNode;

/**
 * This class models a node for the QueryPattern.
 * The PatterNode is connected with a object/relationship node
 * refereed by a TagGrouping. As a result each PatterNode
 * corresponds to one node in the ORM Schema Graph. We dont need
 * to carry this ORMNode as a field in this class. Because with the Label
 * of the TagGrouping we can find the object/relationship node from the ORMSchema.
 */
public class PatternNode implements Cloneable { 
        
    private ORMNode schemaNode;           // The Object/relationship node of the ORMSchemaGraph.    
    private boolean intermediateNode;     // Indicates if the node was an intermediateNode. (An entity not searched by the user)    
    private TagGrouping tagGrouping;      // The Tag Grouping referring to this Node
    
    private Set<OperationAnnotation> operationAnnotations;                 // A set of the Aggregate/GroupBy annotations applied to this patterNode.
    private Set<ConditionAnnotation> conditionAnnotations;                 // A set of the Conditon annotations applied to this patterNode.
    private Set<ComponentRelationAnnotation> componentRelationAnnotations; // A set of the ComponentRelation annotations applied to this patterNode.


    // Indicates if the Query is an aggregate Query and we have to use the according definitions
    // for the Target Node and Condition node.
    private boolean aggregateQuery;  

    /** 
     * Public Constructors. 
     */
    public PatternNode(ORMNode schemaNode, TagGrouping grouping, boolean aggregateQuery) {
        this.schemaNode = schemaNode;
        this.intermediateNode = false;
        this.tagGrouping = grouping;
        this.operationAnnotations = new HashSet<>();
        this.conditionAnnotations = new HashSet<>();
        this.componentRelationAnnotations = new HashSet<>();
        this.aggregateQuery = aggregateQuery;
    }    
    public PatternNode(ORMNode schemaNode) {
        this.schemaNode = schemaNode;
        this.intermediateNode = true;
        this.tagGrouping = null;
        this.operationAnnotations = new HashSet<>();
        this.conditionAnnotations = new HashSet<>();
        this.componentRelationAnnotations = new HashSet<>();
        this.aggregateQuery = false;
    }    


    /**     
     * @return A boolean indicating if this Node is a Target Node.
     */
    public boolean isTargetNode() {
        // Intermediate Nodes are not examined.
        if (this.intermediateNode) return false; 

        // Determine the Function based on the Query (aggregate or non aggregate).
        if (this.aggregateQuery)
            return this.isTargetNodeAggDef();
        else 
            return this.isTargetNodeNonAggDef();
    }


    /**      
     * The node is a Target node in case it is annotated with An Aggregate Function.
     * Use the Definition of the TargetNode in the GroupBy paper (only for aggregate queries)
     * 
     * @return a boolean indicating if this node is an Target node. 
     */
    private boolean isTargetNodeAggDef() {        
        // If the node contains an Aggregate Annotation (whch is not a group by function)
        for (OperationAnnotation annotation: this.operationAnnotations) {            
            if (!annotation.getOperator().isGroupByFunction() )
                return true;
        }        

        return false;
    }


    /**
     * Return if the Node is a Target Node. Use the Definition of the 
     * TargetNode in the ExpressQ paper (only for non aggregate queries).
     * 
     * @return
     */
    private boolean isTargetNodeNonAggDef() {
        // This node is a target node if :
        // Case 1 :  Each Tag must have its condition == null.
        int nullCondTags = 0;
        for (Tag tag: tagGrouping.getTags()) 
            if (tag.getCond() == null)
                nullCondTags++;
        if (nullCondTags == tagGrouping.getTags().size())
            return true;

        // Case 2 :  There is a tag with attribute different than the attribute 
        // of the other tags.
        if (tagGrouping.getTags().size() >= 2)
            for (Tag tag: tagGrouping.getTags())
                if (tag.getAttr() != null) {
                    int countDiffAttrTags = 0;
                    for (Tag innerTag: tagGrouping.getTags()) {
                        if (tag != innerTag && tag.getAttr() != innerTag.getAttr())
                            countDiffAttrTags++;                    
                    }

                    if (countDiffAttrTags == tagGrouping.getTags().size() - 1)
                        return true;
                }

        // Else return false.
        return false;
    }



    /**     
     * @return A boolean indicating if this Node is a Condition Node.
     */
    public boolean isConditionNode() {
        // Intermediate Nodes are not examined.
        if (this.intermediateNode) return false; 

        // Determine the Function based on the Query (aggregate or non aggregate).
        if (this.aggregateQuery)
            return this.isConditionNodeAggDef();
        else 
            return this.isConditionNodeNonAggDef();
    }


    /**
     * The node is a Target node in case it is annotated with A GroupBy Function or
     * an Condition condition.
     * 
     * @return a boolean indicating if this node is a Condition node. 
     */
    public boolean isConditionNodeAggDef() {                
        // A target Node cannot be a condition node too.
        if (this.isTargetNodeAggDef()) return false;

        // If the node contains an Condition Annotations return true.
        if (!this.conditionAnnotations.isEmpty())
            return true;

        // If it contains an GroupBy Operation Annotation return true
        for (OperationAnnotation annotation: this.operationAnnotations)
            if (annotation.getOperator().isGroupByFunction())
                return true;                    
    
        // Else return false.
        return false;
    }


    /**
     * The node is a Target node in case it is annotated with A GroupBy Function or
     * an Condition condition.
     * 
     * @return a boolean indicating if this node is a Condition node. 
     */
    public boolean isConditionNodeNonAggDef() {
        // There must be at least one Tag referring to that node with a condition.
        for (Tag tag: tagGrouping.getTags()) 
            if (tag.getCond() != null)
                return true;
        
        // Else return false.
        return false;
    }

    

    /**
     * @return a boolean indicating if this node is an Object Or Mixed node. 
     */
    public boolean isObjectOrMixedNode() {
        // TODO intermediate nodes must be examined ????
        // Intermediate Nodes are not examined.
        // if (this.intermediateNode) return false; 

        // Return if the Schema Graph corresponding with this node is a Mixed Node or not.
        return this.isObjectNode() || this.isMixedNode();               
    }


    /**
     * In this function we add one more use to the Relationship nodes. A relationship node 
     * may contain some textual attributes also. If a relationship node is annotated with 
     * an condition annotation then it is also an object Node, because it contains information
     * that form an entity and this entity is searched by the user.
     * 
     * @return A boolean indicating if this node is an Object Node.
     */
    public boolean isObjectNode() {
        // A Relationship Node that contains a condition means that it works as an Object node too.
        if (this.schemaNode.isRelationshipNode() && !this.conditionAnnotations.isEmpty())
            return true;        

        // If the above done apply then return is the schema node is an Object Node.
        return this.schemaNode.isObjectNode();
    }

    /**
     * @return A boolean indicating if the node is a Mixed Node.
     */
    public boolean isMixedNode() {
        return this.schemaNode.isMixedNode();
    }

    /**
     * @return A boolean indicating if the node is a Relationship Node.
     */
    public boolean isRelationshipNode() {
        return this.schemaNode.isRelationshipNode();
    }


    /** 
     * Clone the PatterNode 
     * @return A clone of this PatterNode.
     */
    @Override
    public Object clone() {
        PatternNode clone = new PatternNode(this.schemaNode, this.tagGrouping, this.aggregateQuery);
        clone.intermediateNode = this.intermediateNode;
        clone.operationAnnotations = new HashSet<>(this.operationAnnotations);
        clone.conditionAnnotations = new HashSet<>(this.conditionAnnotations);
        clone.componentRelationAnnotations = new HashSet<>(this.componentRelationAnnotations);
        return clone;
    }
    
    /**
     * Print the Node as the node's label.  
     */
    @Override    
    public String toString() {
        String str = this.schemaNode.getRelation().getName() + ":{";

        for (ConditionAnnotation annotation: this.conditionAnnotations) {
            str += annotation.toString() + ", ";
        }
        for (OperationAnnotation annotation: this.operationAnnotations) {
            str += annotation.toString() + ", ";
        }
        for (ComponentRelationAnnotation annotation: this.componentRelationAnnotations) {
            str += annotation.toString() + ", ";
        }

        // Remove the last ","
        if (!this.conditionAnnotations.isEmpty() ||
            !this.componentRelationAnnotations.isEmpty() || 
            !this.operationAnnotations.isEmpty())
                str = str.substring(0, str.length() -2);

        return str + "}";
    }

    /**
     * @return the schemaNode
     */
    public ORMNode getSchemaNode() {
        return schemaNode;
    }

    /**     
     * @return the Object/Relationship Connected with the Node. 
     */
    public String getReferredRelationName() {
        return this.schemaNode.getRelation().getName();
    }

    /**
     * @return true if the node is an Intermediate Node.
     */
    public boolean isIntermediateNode() {
        return this.intermediateNode;
    }


    /**
     * @return a list with all the Condition annotations of the PatterNode.
     */
    public Set<ConditionAnnotation> getConditionAnnotations() {
        return this.conditionAnnotations;
    }


    /**
     * @return a list with all the aggregate annotations of the PatterNode.
     */
    public Set<OperationAnnotation> getOperationAnnotations() {
        return this.operationAnnotations;
    }


    /**
     * @return the ComponentRelationAnnotations
     */
    public Set<ComponentRelationAnnotation> getComponentRelationAnnotations() {
        return componentRelationAnnotations;
    }
    
    /**     
     * @return A boolean indicating if the patternNode is annotated with a ComponentRelation column.
     */
    public boolean containsComponentRelationAnnotation() {
        return !this.componentRelationAnnotations.isEmpty();
    }


    /**         
     * @param annotation The annotation to Add.
     */
    public void addAnnotation(ConditionAnnotation annotation) {
        this.conditionAnnotations.add(annotation);
    }


    /**
     * @param annotation the annotation to add
     */
    public void addAnnotation(OperationAnnotation annotation) {
        this.operationAnnotations.add(annotation);
    }

    /**
     * @param annotation the annotation to add
     */
    public void addAnnotation(ComponentRelationAnnotation annotation) {
        this.componentRelationAnnotations.add(annotation);
    }


    /**     
     * @param annotation The Condition Annotation that my contain in this PatternNode.
     * @return A boolean indicating if this PatternNode contains the parameter annotation.
     */
    public boolean containsAnnotation(ConditionAnnotation annotation) {
        return this.conditionAnnotations.contains(annotation);
    }


    /**
     * @return the tagGrouping
     */
    public TagGrouping getTagGrouping() {
        return tagGrouping;
    }

    /* ======================================================================
     * Note : Dont override hasCode or equals because of graph. A patternNode
     * withe the same Grouping can re enter the graph to connect nodes with 
     * TagGroupings that referrer to the same ORMNode.
     * Each PatterNode must remain unique by the memory it was assigned to it.
     */
}