package shared.database.model.graph;

import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLForeignKeyConstraint;
import shared.database.model.SQLTable;
import shared.util.Graph;
import shared.util.Pair;
import shared.util.Graph.NoLabel;

// This class models an ORM graph.
public class ORMSchemaGraph extends Graph<ORMNode, NoLabel> {
    private static final Logger LOGGER = Logger.getLogger(ORMSchemaGraph.class.getName());

    private HashMap<String,ORMNode> tableToNodeMap;                             // A Mapping between relation's Names and the ORMNodes we created for them.
    private HashMap<String, Set<SQLForeignKeyConstraint>> relationsConstraints; // A mapping between Relation Names an SQLForeignKeyConstraints.    
    private List<SQLTable> componentRelations;                                  // A list holding all the componentRelations.

    /** Public Constructor */
    public ORMSchemaGraph() {
        this.tableToNodeMap = new HashMap<>();
        this.componentRelations = new ArrayList<>();
        this.relationsConstraints = new HashMap<>();
    }

    /**
     * Fill the ORMSchema Graph with the Relations form the SQLDatabase.
     * 
     * @param relations All the Relations of the Database.
     * @param constraints All the constrains that apply between relations.
     */
    public void fill(List<SQLTable> relations, List<SQLForeignKeyConstraint> constraints) {
        // First fill the nodes.                        
        this.fillNodes(relations);                
        
        // Fill the edges of the Graph.        
        this.fillEdges(constraints);        

        // Fill the object and mixed nodes with 
        // their component relations, if any exist.        
        this.attachComponentRelations();        
    }


    /**
     * Fill the nodes of the ORMSchema Graph. Each node is assigned with a type depending on 
     * the Relation R that it corresponds to and this relations attributes.
     * 
     * There are 4 Types [Object, Relationship, Mixed, Component] and 4 Cases :    
     * 1. A relation R is an object relation if there exists some relation R that references R,
     *    and R does not reference other relations.
     * 2. A relationship R is a relationship relation if the primary key of R comprises more
     *     than one foreign key.     
     * 3. A relation R is a mixed relation if (a) there exists two relations R' and R''	
     *    such that R' references R and R references R'', and (b) the primary key of
     *    R does not contain more than one foreign key.
     * 4. A relation R1 is a component relation if (a) no relation references R1,(b)
     *    the primary key of R1 does not contain more than one foreign key.
     *     
     * @param relations The Relations in the SQL Database.
     */
    private void fillNodes(List<SQLTable> relations) {

        // Loop all the relations and create a node for each one, if possible.
        for (SQLTable rel: relations) {        
            // There are 4 cases depending on the relation type ( Object, component, mixed, relationship) {
            ORMNode node = null;
            boolean componentRel = false;

            // Case 1.
            if (rel.getReferencingConstraints().isEmpty() && !rel.getReferencedConstraints().isEmpty()) {
                node = new ORMNode(ORMNode.NodeType.Object, rel); // Create an Object Node.                
            }
            // Case 2.
            else if (!rel.getReferencingConstraints().isEmpty() && rel.getReferencedConstraints().isEmpty()
                     && rel.getForeignKeys().size() > 1) {                
                node = new ORMNode(ORMNode.NodeType.Relationship, rel); // Create an Relationship Node.                
            }
            // Case 3.
            else if (!rel.getReferencedConstraints().isEmpty() && !rel.getReferencingConstraints().isEmpty() &&
                     rel.getNumOfFKsInPK() <= 1)
                node = new ORMNode(ORMNode.NodeType.Mixed, rel); // Create an Relationship Node.                            
            // Case 4.
            else if (rel.getReferencedConstraints().isEmpty() && !rel.getReferencingConstraints().isEmpty() &&
                     rel.getForeignKeys().size() <= 1) {
                this.componentRelations.add(rel);  // Store the componentRelations for later.
                componentRel = true;
            }

            // In node == null log else add the node and the mapping.
            if (node == null && !componentRel) {
                LOGGER.warning("There relation \"" + rel.getName() + "\" doesn't fall in any of the " +
                            "Types: {Object, Relationship, Mixed, Component}");
            } else if (!componentRel) {
                this.addNode(node); 
                this.tableToNodeMap.put(rel.getName(), node); // Keep the rel to node mapping.
            }
        }
    }


    // Fills the edges of the graph.
    private void fillEdges(List<SQLForeignKeyConstraint> constraints) {
        // First loop all the fkConstraints to add the edged one by one.
        for (SQLForeignKeyConstraint constraint: constraints) {
            ORMNode startNode = this.tableToNodeMap.get(constraint.getPrimaryKeyColumn().getTable().getName());
            ORMNode endNode = this.tableToNodeMap.get(constraint.getForeignKeyColumn().getTable().getName());

            // An componentRelation is not on the tableToNodeMap cause they dont correspond to
            // an ORMNode. So start node or end node will be null. Just skip this constraint.
            if (startNode == null || endNode == null) continue;

            // Update the relation to constraints mapping.
            this.updateRelationToConstraintsMap(startNode, constraint);
            this.updateRelationToConstraintsMap(endNode, constraint);
            
            // Add the connection in the database.
            this.addUnDirEdge(startNode, endNode);
        }
    }


    /** 
     * Fill the ORMNodes with the relations denoted as component relations. Each Object/Mixed node may 
     * contain some Component Relations. 
     */    
    private void attachComponentRelations() {
        // Loop all the component Relations.
        for (SQLTable componentRelation: this.componentRelations) {
            // For each relation that the componentRelation references attach it to its respective node.
            for (SQLForeignKeyConstraint constraint: componentRelation.getReferencingConstraints()) {
                // Get the node of the the relation that componentRelation references.
                ORMNode node = this.tableToNodeMap.get(constraint.getPrimaryKeyColumn().getTable().getName());
                node.addComponentRelation(componentRelation, constraint);
            }
        }
    }


    /**
     * Update the relation to constraint mapping of the relation referred
     * by the param node to contain one more constrain , the parameter constraint.
     * 
     * @param node the node referring to an SQLTable.
     * @param constraint a constraint involving this table.
     */
    public void updateRelationToConstraintsMap(ORMNode node, SQLForeignKeyConstraint constraint)  {
        // Get the Constraints so far or a new List.
        Set<SQLForeignKeyConstraint> constraintsSoFar = this.relationsConstraints.getOrDefault(
            node.getRelation().getName(),
            new HashSet<>() 
        );

        // Append the new constraint.
        constraintsSoFar.add(constraint);

        // Update or Add the new Mapping.
        this.relationsConstraints.put(
            node.getRelation().getName(),
            constraintsSoFar
        );
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
    public int getDistanceBetweenNodes(String nodeARelationName, String nodeBRelationName) {
        ORMNode nodeA = this.getORMNode(nodeARelationName);
        ORMNode nodeB = this.getORMNode(nodeBRelationName);        
        int distance = 0;

        // Get The Patch connecting those two nodes.        
        Graph<ORMNode, NoLabel> path = this.getPathConnecting2Nodes(nodeA, nodeB);
        for (ORMNode node: path.getVertexes())
            if (node.isObjectNode() || node.isMixedNode())
                distance++;
        
        // Return the distance.
        return distance;
    }


    /**
     * Get the columns joining left and right ORMNode. To get the columns we need to 
     * find the SQLForeignKeyConstraint between those two nodes and extract the right columns.
     * 
     * @param leftNode
     * @param rightNode
     * @return
     */
    public Pair<SQLColumn, SQLColumn> getColumnsJoiningNodes(ORMNode leftNode, ORMNode rightNode) {
        // Find all the constraints that leftNode and rightNode involve.
        Set<SQLForeignKeyConstraint> leftNodeConstraints = this.relationsConstraints.get(leftNode.getRelation().getName());
        Set<SQLForeignKeyConstraint> rightNodeConstraints = this.relationsConstraints.get(rightNode.getRelation().getName());
        
        // Find the one constraint that both are involved.
        for (SQLForeignKeyConstraint leftNodeConstraint: leftNodeConstraints)
            for (SQLForeignKeyConstraint rightNodeConstraint: rightNodeConstraints)
                if (leftNodeConstraint == rightNodeConstraint)
                    return leftNodeConstraint.getColumnPairIfTablesPatricipateInConstraint(
                        leftNode.getRelation(), rightNode.getRelation(), null
                    );


        return null;
    }


    /**
     * @param relationName The name of the Relation whose corresponding ORMNode we want to return.
     * @return The ORMNode corresponding to parameter relationName.
     */    
    public ORMNode getORMNode(String relationName) {
        return this.tableToNodeMap.get(relationName);
    }


    /**    
     * @param relationName
     * @return A boolean indicating if the relation with the above name is a component Relation.
     */    
    public boolean isComponentRelation(String relationName) {
        for (SQLTable rel: this.componentRelations) 
            if (rel.getName().equals(relationName))
                return true;
        return false;
    }


    /**
     * @param relationName The name of the component Relation
     * @return The component Relation with the above name or null.
     */
    public SQLTable getComponentRelation(String relationName) {
        for (SQLTable rel: this.componentRelations) 
            if (rel.getName().equals(relationName))
                return rel;
        return null;
    }

    
    /**
     * @return All the Object/Mixed nodes in the graph.
     */    
    public List<ORMNode> getAllObjectOrMixedNodes() {
        List<ORMNode> objectMixedNodes = new ArrayList<>();
        for (ORMNode node: this.getVertexes())
            if (node.isObjectNode() || node.isMixedNode())
                objectMixedNodes.add(node);        
        return objectMixedNodes;
    }
    
}