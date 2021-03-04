package expressq2.components;

import java.util.List;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.database.model.graph.ORMNode;
import shared.database.model.graph.ORMSchemaGraph;
import expressq2.model.OperationAnnotation;
import expressq2.model.AnnotatedQuery;
import expressq2.model.ComponentRelationAnnotation;
import expressq2.model.ConditionAnnotation;
import expressq2.model.Keyword;
import expressq2.model.PatternNode;
import expressq2.model.QueryPattern;
import expressq2.model.Tag;

/**
 * This class models an QueryPatterAnnotator. Its functions annotate a query
 * Pattern with AggregateAnnotations, ComponentRelationAnnotations and
 * EqualityAnnotations.
 */
public class QueryPatternAnnotator {


    /**
     * Annotated the list of query Patterns that correspond to the annotatedQuery.
     * Use two rules referred to the GROUP BY paper page 4 section 3.1.1
     * 
     * @param queryPatterList the patterns to annotate.
     * @param annotatedQuery the annotated Query.
     * @param schemaGraph the ORM Schema Graph of the database.
     */
    public static void annotateQueryPattern(QueryPattern queryPattern, AnnotatedQuery annotatedQuery, ORMSchemaGraph schemaGraph) {
        List<Keyword> keywords = annotatedQuery.getKeywords();   // Get all the keywords of the Annotated Query.

        // Firs loop all the operator Term keywords in the annotated query.
        List<Integer> operatorKeywordsIndices = annotatedQuery.getOperatorKeywordsIndices();
        for (Integer index: operatorKeywordsIndices) {
            Keyword keyword = keywords.get(index);            // Get the operator keyword.
            Keyword nextKeyword = keywords.get(index + 1);    // Get the keyword after the operator keyword.
            Keyword prevKeyword = null;                       // The previous Keyword of the operator keyword.

            if (nextKeyword.isAggregateFunction()) continue;      // Skip the nesting keyword. (Aggregate over an Aggregate Term)                        
            if (index > 0) prevKeyword = keywords.get(index - 1);
            
            // Annotate the QueryPattern with aggregation.
            annotateWithOperation(annotatedQuery, queryPattern, schemaGraph, keyword, nextKeyword, prevKeyword);
        }

        // Then loop all the keywords referring to a Component Relation.
        List<Integer> componentRelationReferredKeywordIndices = annotatedQuery.getComponentRelationReferredKeywordsIndices();
        for (Integer index: componentRelationReferredKeywordIndices) {
            Keyword keyword = annotatedQuery.getKeywords().get(index);      // Get the keyword.
            Tag tag = annotatedQuery.getKeywordsTag(keyword);               // Get the Tag linked with keyword.            
            PatternNode patternNode = queryPattern.getPatterNode(keyword);  // Get the Pattern node linked with this Keyword.

            // Annotate the patternNode with a ComponentRelation Annotation.
            annotateWithComponentRelation(patternNode, tag);        
        }  
        
        // Finally Loop all the Keywords referring to a Value.
        List<Integer> valueReferringKeywordsIndicies = annotatedQuery.getValueReferredKeywordsIndices();
        for (Integer index: valueReferringKeywordsIndicies) {                 
            Keyword keyword = annotatedQuery.getKeywords().get(index);      // Get the keyword.
            Tag tag = annotatedQuery.getKeywordsTag(keyword);               // Get the Tag linked with keyword.            
            PatternNode patternNode = queryPattern.getPatterNode(keyword);  // Get the Pattern node linked with this Keyword.

            // Annotate the patterNode with an Equality Annotation.
            annotateWithEquality(patternNode, tag);
        }

    }


    /**
     * Create an ComponentRelation Annotation for the patternNode. The 
     * ComponentRelation will be extracted from the Tag.
     * 
     * @param patternNode the patterNode to Annotate
     * @param tag the Tag referring to a componentRelation
     */
    public static void annotateWithComponentRelation(PatternNode patternNode, Tag tag) 
    {
        String componentRelationName = tag.getComponentRelationName();  
        // SQLTable componentRelation = null;

        // // Find the ComponentRelation with the above name
        // for (SQLTable componentRel: patternNode.getSchemaNode().getComponentRelations()) 
        //     if (componentRel.getName().equals(componentRelationName)) {
        //         componentRelation = componentRel;
        //         break;
        //     }
        
        // Create the annotation.
        ComponentRelationAnnotation annotation = new ComponentRelationAnnotation(componentRelationName);

        // Add it in the patternNode.
        patternNode.addAnnotation(annotation);
    }


    /**
     * Annotate the parameter PatternNode with an Equality Annotation. The annotation is extracted 
     * from the tag.
     * 
     * @param patternNode The node to annotate.
     * @param tag The tag  indicating the annotation.
     */
    public static void annotateWithEquality(PatternNode patternNode, Tag tag) {        
        SQLColumn column  = patternNode.getSchemaNode().getAttributeByName(tag.getAttr()); // Get the Column of the Equality condition.
        String value = tag.getCond();                                                      // Get the Value of the Equality condition.

        // Add the annotation
        patternNode.addAnnotation(new ConditionAnnotation(column, value, tag.getAppearances()));        
    }
    

    /**
     * Add an AggregateAnnotation in the PatternNode linked with the parameter next keyword.
     * The parameter keyword should be an Aggregate Term. Also if the prevKeyword of the parameter
     * keyword is an Aggregate Term then annotate the QueryPattern with a nested Aggregation 
     * associating with the results of the aggregation in the PatternNode linked with the 
     * parameter next keyword.
     * 
     * @param annotatedQuery
     * @param queryPattern
     * @param schemaGraph
     * @param keyword
     * @param nextKeyword
     * @param prevKeyword
     */
    public static void annotateWithOperation(
      AnnotatedQuery annotatedQuery, 
      QueryPattern queryPattern, 
      ORMSchemaGraph schemaGraph,
      Keyword keyword, Keyword nextKeyword, Keyword prevKeyword) 
    {                
        Tag tag = annotatedQuery.getKeywordsTag(nextKeyword);              // Get the Tag linked with nextKeyword.
        PatternNode patternNode = queryPattern.getPatterNode(nextKeyword); // Get the Pattern node linked with this Keyword.
        ORMNode schemaNode = patternNode.getSchemaNode();                  // Get the SchemaNode referred by the keyword.
        OperationAnnotation operationAnnotation = null;                    // The operation annotation to add to the PatterNode.

        // If the Next Term of an operator term refers to an object/Mixes/Relationship node (SQLRelation),
        // annotate that Term's patterNode with the PK of the Relation and the Operation we must apply to it.
        if (tag.refersToRelation() && !schemaGraph.isComponentRelation(tag.getLabel())) {
            SQLTable relation = schemaNode.getRelation();                             // The relation referred by the keyword.
            SQLColumn primaryKey = (SQLColumn) relation.getPrimaryKey().toArray()[0]; // The relation's primary Key.

            // Set the annotation to the patterNode.
            operationAnnotation =  new OperationAnnotation(primaryKey.getName(), keyword);
        }
        // Else if the Next Term refers to an Attribute Name or a component Relation annotate the Term's
        // patterNode with the Attribute of the Relation (in case of attribute) or The PK of the ComponentRelation
        // and the Operation we must apply to it.
        else if (tag.refersToRelation() && schemaGraph.isComponentRelation(tag.getLabel())) {
            SQLTable relation = schemaGraph.getComponentRelation(tag.getLabel()); // The relation referred by the keyword.
            SQLColumn primaryKey = (SQLColumn) relation.getPrimaryKey().toArray()[0]; // The relation's primary Key.            
            
            // Set the annotation to the patterNode.
            operationAnnotation =  new OperationAnnotation(primaryKey.getName(), keyword);
        }
        else if (tag.refersToAttribute()) {            
            SQLColumn column = patternNode.getSchemaNode().getAttributeByName(tag.getAttr()); // The attribute referred by the keyword.            

            // Set the annotation to the patterNode.
            operationAnnotation =  new OperationAnnotation(column.getName(), keyword);
        }                

        // Else if the keyword is an aggregate function and the previous Keyword is an aggregate 
        // function too then we have a nested Aggregation Query. Annotate the whole QueryPattern 
        // with an AggregateAnnotation applied on the result of this keyword's Aggregate Function.
        if (prevKeyword != null && keyword.isAggregateFunction() && prevKeyword.isAggregateFunction()) {
            String column = operationAnnotation.getAlias();      // The Columns is the result of the Aggregate query.

            // Set the annotation to the QueryPattern.
            queryPattern.setNestedAggregateAnnotation(new OperationAnnotation(column, prevKeyword));
        }

        // Add the Aggregate annotation to the PatterNode.
        if (operationAnnotation != null) 
            patternNode.addAnnotation(operationAnnotation);        
    }


}