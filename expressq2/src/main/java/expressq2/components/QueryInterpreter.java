package expressq2.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.connectivity.thor.response.Table;
import shared.database.model.graph.ORMNode;
import shared.database.model.graph.ORMSchemaGraph;
import expressq2.model.AnnotatedQuery;
import expressq2.model.PatternNode;
import expressq2.model.QueryPattern;
import expressq2.model.TagGrouping;
import shared.util.Graph;
import shared.util.Graph.NoLabel;

// Inputs: A set of tag groupings S, ORM schema graph G
// Outputs: List of query patterns.
// 
// This class Models the Query Interpreter of the ExpressQ paper.
// Using the tag groupings create a query patter. A Query Pattern is 
// is a minimal connected graph that represents one possible 
// search intention of the query. The Graph contains one node 
// representing the object/relation refereed by each tag grouping.
public class QueryInterpreter { 
    
    // Public constructor.
    public QueryInterpreter() {}

    // Return all the QueryPatterns corresponding on the query Interpretations. Each queryPattern
    // denotes a different query interpretation.
    public static List<QueryPattern> interpret(AnnotatedQuery annotatedQuery, ORMSchemaGraph schemaGraph) {
        Set<TagGrouping> tagGroupings = annotatedQuery.getTagGroupings();
        Set<ORMNode>      schemaNodes = new HashSet<>();             // A set of all the ORMNodes.
        Set<PatternNode>  patternNodes = new HashSet<>();            // A set of all the pattern nodes.
        List<QueryPattern>  queryPatternList = new ArrayList<>();    // A list of all the possible query patters.        
        
        // Create and Initialize a Query Pattern bases on the annotated Query.
        QueryPattern queryPattern = new QueryPattern();
        queryPattern.setPenaltyPoints(annotatedQuery.getPenaltyPoints());
        queryPattern.setAggregateQuery(annotatedQuery.isAggregateQuery());

        // Each grouping referrers to an Object/Relationship/Mixed Node. Create a patternNode
        // referring to that grouping and also referring to an ORMNode in the schema graph.
        for (TagGrouping grouping: tagGroupings) {
            ORMNode schemaNode = schemaGraph.getORMNode(grouping.getLabel());                                    // The schemaNode corresponding to the Entity of the TagGrouping.
            PatternNode patternNode = new PatternNode(schemaNode, grouping, annotatedQuery.isAggregateQuery());  // The new PatternNode corresponding to the same Entity as the SchemeNode.            

            // Store both nodes inside two sets.
            patternNodes.add(patternNode);
            schemaNodes.add(schemaNode);

            // Add the new patterNode to the QueryPattern and link it with all
            // the keywords referring to the same entity as patterNode refers.
            queryPattern.addNode(patternNode);
            queryPattern.linkKeywordsWithPatterNode(grouping.getKeywords(), patternNode);
        }

        
        // If there is only one patterNode then the only queryPatter returned is a pattern containing one node.
        if (patternNodes.size() == 1) {
            queryPatternList.add(queryPattern);
        }
        // Else if there is a 1 to 1 relation between patternNodes and schemaNodes
        // find the subGraph in the ORMSchemaGraph containing all the schemaNodes and based 
        // on that subGraph and the 1-1 relation complete the QueryPattern.
        else if (patternNodes.size() == schemaNodes.size()) {
            Graph<ORMNode, NoLabel> subGraph = schemaGraph.subGraph(new HashSet<>(schemaNodes));  // Get the SubGraph from the SchemaGraph.
            
            // Complete the QueryPattern with the nodes and edges of the subGraph.
            completeQueryPatternLikeGraph(queryPattern, subGraph, schemaNodes, patternNodes);

            // Add the Pattern to the PatternList.
            queryPatternList.add(queryPattern);
        }
        // Else if there are some schemaNodes that correspond to more than one PatternNode.
        // This happens because different TagGroupings can refer to the same Object/relationship node.
        // To Create the QueryPattern, cluster the TagGroupings to refer to the same node and try connecting the clusters.
        else if (patternNodes.size() > schemaNodes.size()) {            
            List<Cluster> clusters = clusterPatternNodes(patternNodes);  // Cluster the Groupings and order them by their acceding size.

            // If the smallest cluster contains only one TagGrouping connect that cluster with all other clusters.
            if (clusters.get(0).size() == 1) {
                // Get the schemaNode corresponding to the patternNodes of the first cluster.
                // All the other clusters will connect to this node.
                PatternNode firstClustersPatterNode = clusters.get(0).nodes.get(0);
                ORMNode interconnectNode = firstClustersPatterNode.getSchemaNode();

                // Create a subList of the clusters, containing all the clusters except the first one.
                List<Cluster> clustersSubList = clusters.subList(1, clusters.size());

                // Two lists storing patternNodes and their corresponding schemaNodes.
                // Those patternNodes are contained in the QueryPattern.
                List<ORMNode> containedSchemaNodes = new ArrayList<>();
                List<PatternNode> containedPatternNodes = new ArrayList<>();
                containedSchemaNodes.add(interconnectNode);
                containedPatternNodes.add(firstClustersPatterNode);
                
                // Connect all the clusters with the first cluster via the interconnectNode.
                connectClustersAccordingToNode(
                    clustersSubList, interconnectNode, 
                    containedSchemaNodes, containedPatternNodes, 
                    schemaGraph, queryPattern
                );

                // Add the pattern to the patternList.
                queryPatternList.add(queryPattern);
            }
            // Else try to find a node to connect him with all the clusters.
            else {
                // Get all the object/mixed nodes and remove the nodes already used for the QueryPattern.
                List<ORMNode> possibleNodes = schemaGraph.getAllObjectOrMixedNodes();
                possibleNodes.removeAll(schemaNodes);

                // For every possible node create a QueryPattern using the interconnectNode to connect all clusters.
                for (ORMNode interconnectNode: possibleNodes) {                    
                    QueryPattern clonedPattern = queryPattern.shallowClone();  // Clone the QueryPattern.

                    // Create a patternNode corresponding to the interconnectNode.
                    PatternNode patternNode = new PatternNode(interconnectNode);

                    // Two lists storing patternNodes and their corresponding schemaNodes.
                    // Those patternNodes are contained in the QueryPattern.
                    List<ORMNode> containedSchemaNodes = new ArrayList<>();    
                    List<PatternNode> containedPatternNodes = new ArrayList<>();
                    containedSchemaNodes.add(interconnectNode);
                    containedPatternNodes.add(patternNode);

                    // Add the new patterNode to the clonedPattern.
                    clonedPattern.addNode(patternNode);
                    
                    // Connect all the clusters with the the interconnectNode.
                    connectClustersAccordingToNode(
                        clusters, interconnectNode,
                        containedSchemaNodes, containedPatternNodes,
                        schemaGraph, clonedPattern
                    );

                    // Add the pattern to the patternList.
                    queryPatternList.add(clonedPattern);
                }
            }
        }
        // System.out.println("[INFO] QP creation " + t1);
        
        List<QueryPattern> patternsToReturn = new ArrayList<>();   // Stores all the Patterns to be returned.

        // We annotate only the queryPattern variable. Not all the QueryPatterns in the PatternList 
        // because every node that needs to be annotated is a node in the queryPattern variable
        // and every other QueryPattern in the queryPatternList is a shallow copy of the 
        // queryPattern variable.                
        QueryPatternAnnotator.annotateQueryPattern(queryPattern, annotatedQuery, schemaGraph);        

        // For each QueryPattern Annotate it , then Disambiguate it and keep it's meanings in the above list.                
        for(QueryPattern pattern: queryPatternList) {
            // NOTE : removed the disambiguator it's not good
            // patternsToReturn.addAll( QueryPatternDisambiguator.disambiguateQueryPattern(pattern, schemaGraph) );
            patternsToReturn.add(pattern);
        }        

        // Return the Query Pattern List.
        return patternsToReturn;
    }
       
  
    // Connects all the cluster with paths starting from the parameter interconnectNode and 
    // ending in every node inside a cluster. The above connections are updating the queryPatter.        
    private static void connectClustersAccordingToNode(
        List<Cluster> clusters, ORMNode interconnectNode,
        List<ORMNode> containedSchemaNodes, List<PatternNode> containedPatterNodes,
        ORMSchemaGraph schemaGraph, QueryPattern queryPattern) 
    {        
        // Loop all the other Clusters and try to connect them with the interconnectNode.
        for (Cluster cluster: clusters) {                        
            // Get the schemaNode corresponding to the first patternNode of the cluster.            
            // We could use any patternNode in the cluster, all correspond to the same schemaNode.
            ORMNode schemaNode = schemaGraph.getORMNode(cluster.nodes.get(0).getReferredRelationName());
            List<ORMNode> distinctContainedSchemaNodes = new ArrayList<>(containedSchemaNodes);
            distinctContainedSchemaNodes.add(schemaNode);

            // Find a path connecting the interconnectNode with the
            // schemaNode corresponding to the cluster's patterNodes.            
            Graph<ORMNode, NoLabel> path = schemaGraph.getPathConnecting2Nodes(interconnectNode, schemaNode);            

            // All the patternNodes in the cluster correspond to one SchemaNode, but are 
            // distinct entities. So we need to add the path in the QueryPattern like 
            // the path in the ORMSchemaGraph above, but to all the patternNodes in 
            // this cluster. The path is exactly the same, only the ending PatterNode changes.
            // With this loop we keep the 1 to 1 relationship between SchemaNodes and PatternNodes
            // because we separate the patternNodes corresponding to the same SchemaNode.
            for (PatternNode node: cluster.nodes) {
                // A list storing the parameter patterNodes along with each PatterNode of the Cluster.
                // These keeps the containedSchemaNode and the containedPatterNodes in a 1-1 rel.
                List<PatternNode> distinctContainedPatternNodes = new ArrayList<>(containedPatterNodes);                
                distinctContainedPatternNodes.add(node);                

                // Complete the QueryPattern like the path extracted from the ORMSchemaGraph.
                completeQueryPatternLikeGraph(
                    queryPattern, path, distinctContainedSchemaNodes, distinctContainedPatternNodes
                );
            }                    
        }
    }

    // Complete the QueryPattern graph like the parameter Graph. The Pattern Nodes have a 
    // 1 to 1 relationship with the ORMSchemaNodes and the edges can be attached according
    // to this 1 to 1 matching.
    private static void completeQueryPatternLikeGraph(
        QueryPattern queryPattern, Graph<ORMNode, NoLabel> graph,
        Collection<ORMNode> containedSchemaNodes, Collection<PatternNode> containedPatterNodes) 
    {
        // Create a dictionary mapping ORMNodes to PatternNodes.
        NodeDictionary nodeDictionary = new NodeDictionary(containedSchemaNodes, containedPatterNodes);

        // Get the Intermediate Nodes of the graph, simply by removing the 
        // schemaNodes already with corresponding nodes in the queryPattern,
        // from all the graph nodes.
        List<ORMNode> intermediateNodes = graph.getVertexes();
        intermediateNodes.removeAll(containedSchemaNodes);

        // For each Intermediate Node ORMNode create an new PatternNode.
        for (ORMNode intermediateNode: intermediateNodes) {
            PatternNode newPatterNode = new PatternNode(intermediateNode);

            // Add the PatternNode to the query pattern and to the mapping.
            queryPattern.addNode(newPatterNode);
            nodeDictionary.addMapping(intermediateNode, newPatterNode);
        }

        // Then for each edge on the graph create an edge on the QueryPattern.
        for (Graph<ORMNode, NoLabel>.Edge edge: graph.getEdges()) {
            if (!queryPattern.areUnDirConnected(
                    nodeDictionary.getPatternNode(edge.getStartNode()),
                    nodeDictionary.getPatternNode(edge.getEndNode()))
               ) 
                // Use the Mapping to get the patternNodes that correspond to ORMNodes.
                queryPattern.addUnDirEdge( 
                    nodeDictionary.getPatternNode(edge.getStartNode()),
                    nodeDictionary.getPatternNode(edge.getEndNode())
                );
        }
    }


    // Clusters all the TagGroupings that refer to the same label and 
    // return a list of clusters ordered by ascending cluster cardinality.
    private static List<Cluster> clusterPatternNodes(Collection<PatternNode> patternNodes) {
        HashMap<String, Cluster> clusters = new HashMap<>();  // The list of clusters along with their labels.

        // Loop all the patternNodes and Add them to clusters.
        for (PatternNode patterNodes: patternNodes) {
            // Check if there is a cluster where this patterNodes can be inserted.
            Cluster matchedCluster = clusters.get(patterNodes.getReferredRelationName());

            // If there is a cluster insert it there , else create a new one.
            if (matchedCluster != null)
                matchedCluster.addPatternNode(patterNodes);
            else 
                clusters.put(patterNodes.getReferredRelationName(), new Cluster(patterNodes));
        }

        // Return the Clusters.
        List<Cluster> sortedClusters = new ArrayList<>(clusters.values());
        Collections.sort(sortedClusters, new Cluster.CardinalityComparator());
        return sortedClusters;
    }


    /**
     * @param pattersProduced
     * @return A {@link Table} containing statistics about the execution of the component and the results ir produced
     */
    public static Table getStatistics(List<QueryPattern> pattersProduced) {
        List<Table.Row> rows = new ArrayList<>();        // The table rows.
        Integer avgNumOfNodes = 0;                       // Average number of nodes in patters

        // Compte avg num of nodes
        for (QueryPattern patter: pattersProduced) {
            avgNumOfNodes += patter.getVertexes().size();
        }
        avgNumOfNodes = (pattersProduced.size() != 0) ? avgNumOfNodes / pattersProduced.size() : 0;

        // Add the number of patterns Produces
        rows.addAll(Arrays.asList(
            new Table.Row(
                Arrays.asList("QueryPatternGraphs created" , Integer.toString(pattersProduced.size()))
            ),
            new Table.Row(
                Arrays.asList("Average number of nodes in QueryPatternGraphs" , avgNumOfNodes.toString())
            )
        ));

               
        // Return the table containing the Components Info.
        return new Table(rows);
    }


}


// Models a TagGroupings Cluster. The cluster stores Groupings That refer 
// to the same object/relationship node in the ORMSchemaGraph.
class Cluster{

    // Cardinality Comparator for the Clusters.
    public static class CardinalityComparator implements Comparator<Cluster> {
        @Override
        public int compare(Cluster a, Cluster b) {
            if (a.nodes.size() < b.nodes.size())
                return -1;
            else if (a.nodes.size() == b.nodes.size())
                return 0;
            else 
                return 1;            
        }
    }

    List<PatternNode> nodes;  // A list of groupings stored in the Cluster.

    // Public constructor.
    Cluster(PatternNode node) {
        this.nodes = new ArrayList<>();
        this.nodes.add(node);
    }

    // Add a node
    boolean addPatternNode(PatternNode node) {
        return this.nodes.add(node);
    }
    
    // Get the label of the cluster. It is used to identify if 
    // there a node can be inserted to this cluster.
    String getClusterLabel() {
        return this.nodes.get(0).getReferredRelationName();
    }

    // Get the cluster size.
    int size() {
        return this.nodes.size();
    }

    @Override
    public String toString() {
        return "c: {" + nodes.toString() + "}";
    }
}

// A Dictionary mapping SchemaNodes to PatternNodes. Only on cases where there is 
// an 1 to 1 relationship between ORMNodes and PatterNodes.
class NodeDictionary {    
    HashMap<ORMNode, PatternNode> ormToPatterNodesMapping;

    // Constructor
    NodeDictionary(Collection<ORMNode> schemaNodes, Collection<PatternNode> patternNodes) {
        this.ormToPatterNodesMapping = new HashMap<>();
        
        // Initialize the hashMap.
        for(ORMNode schemaNode: schemaNodes)
            for (PatternNode patternNode: patternNodes)
                if (patternNode.getReferredRelationName().equals(schemaNode.getRelation().getName())) {
                    this.ormToPatterNodesMapping.put(schemaNode, patternNode);
                    break;
                }

    }

    // Add NodesMapping to the dictionary.
    void addMapping(ORMNode schemaNode, PatternNode patternNode) {
        this.ormToPatterNodesMapping.put(schemaNode, patternNode);
    }
 
    // Get the PatternNode connected with the parameter SchemaNode.
    PatternNode getPatternNode(ORMNode schemaNode){
        return this.ormToPatterNodesMapping.get(schemaNode);
    }

}

