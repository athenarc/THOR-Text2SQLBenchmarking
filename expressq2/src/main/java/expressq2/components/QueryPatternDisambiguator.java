package expressq2.components;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.database.model.graph.ORMSchemaGraph;
import expressq2.model.OperationAnnotation;
import expressq2.model.ConditionAnnotation;
import expressq2.model.Keyword;
import expressq2.model.PatternNode;
import expressq2.model.QueryPattern;

/**
 * This class Models the QueryPattern Disambiguator. It inputs a QueryPattern and disambiguates it.
 * Disambiguating a queryPattern means creating more possible meanings of that pattern.
 * The Disambiguation technique distinguishes objects that are referred by the same keyword.
 * 
 * So there are two possible Meanings on a pattern containing keywords that refer to many Objects: 
 * - Apply the aggregation functions to every distinct object. 
 * - Apply the aggregate functions to all the objects that referred by the same keyword.
 * 
 * Note: Use the algorithm described in GROUP BY paper page 4 section 3.1.2. 
 * GROUP BY paper :( https://openproceedings.org/2016/conf/edbt/paper-39.pdf )
 */
public class QueryPatternDisambiguator {


     /**
     * Disambiguate a QueryPattern And return it possible meanings.
     * 
     * @param queryPatter the pattern to disambiguate.     
     * @param schemaGraph the ORM Schema Graph of the database.
     * @return 
     */
    public static Collection<QueryPattern> disambiguateQueryPattern(QueryPattern queryPattern, ORMSchemaGraph schemaGraph) {                
        Set<QueryPattern> possiblePatternMeanings = new HashSet<>();   // A set storing possible meanings of the queryPattern.            
        possiblePatternMeanings.add(queryPattern);
        
        // Search for multiple Objects referred by the same Equality Annotation
        // in relations corresponding to the patterNodes of the Query Pattern.
        for (PatternNode patterNode: queryPattern.getVertexes()) {
            if (patterNode.isIntermediateNode()) continue;  // Skip the intermediate Nodes. They are not annotated.

            // Loop all the Equality Annotations of the patterNode.
            for (ConditionAnnotation annotation: patterNode.getConditionAnnotations()) {

                // Get how many times it appears 
                int valueAppearances = annotation.getAppearances();

                // Create a copy for each QueryPatter in the possibleMeanings List and annotate
                // each one with a GroupBy(rel.pk) annotation to distinguish the objects in the pattern.
                if (valueAppearances > 1) {
                    Set<QueryPattern> clonedPatterns = new HashSet<>();  // All the clones of the possible Patterns.
                    for (QueryPattern pattern: possiblePatternMeanings) {                            
                        QueryPattern patternClone = pattern.deepClone(null);              // The QueryPattern Clone.
                        SQLTable relation = annotation.getAttribute().getTable();         // The relation referred by the Annotation.
                        Set<SQLColumn> primaryKey = relation.getPrimaryKey();             // The relation's primary Key.                                                        
                        SQLColumn selectedColumn = (SQLColumn) primaryKey.toArray()[0];   // Pick one Column from the PrimaryKey (the first).

                        // Find the cloned patternNode in the cloned queryPattern.
                        PatternNode clonedNode = patternClone.getPatterNode(annotation);

                        // Annotate that node with a GroupBy(rel.pk) annotation.
                        clonedNode.addAnnotation(new OperationAnnotation(selectedColumn.getName(), new Keyword(Keyword.GROUPBY_TERM)) );

                        // Add the clone to the set.
                        clonedPatterns.add(patternClone);
                    }                        
                    possiblePatternMeanings.addAll(clonedPatterns);
                }
            }
        }

        // Return all the possible meanings of the Pattern.
        return possiblePatternMeanings;
    }

}