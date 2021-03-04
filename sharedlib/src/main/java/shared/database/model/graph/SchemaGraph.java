package shared.database.model.graph;

import shared.database.model.SQLColumn;
import shared.database.model.SQLForeignKeyConstraint;
import shared.database.model.SQLTable;
import shared.util.Graph;
import shared.util.Pair;
import shared.util.Graph.NoLabel;

import java.util.List;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

// This class models the database as a graph to depict
// the foreign key constraints between the tables of the database.
public class SchemaGraph extends Graph<SQLTable, NoLabel> {

    private HashMap<String,SQLTable> nameToRelationMap;                         // A Mapping between relation's Names and the ORMNodes we created for them.
    private HashMap<String, Set<SQLForeignKeyConstraint>> relationsConstraints; // A mapping between Relation Names an SQLForeignKeyConstraints.        

    public SchemaGraph() {
        super();
        this.nameToRelationMap = new HashMap<>();
        this.relationsConstraints = new HashMap<>();
    }    

    // Copy Constructor.
    public SchemaGraph(SchemaGraph copyGraph) {
        // Deep Copy the adjacency Matrix 
        super();
        super.cloneLikeGraph(copyGraph, null);

        // Shallow Copy the Table Names
        this.nameToRelationMap = new HashMap<>(copyGraph.nameToRelationMap);

        // Shallow Copy the Constrains
        this.relationsConstraints = new HashMap<>(copyGraph.relationsConstraints);
    }
    
    // Fills the graph.
    public void fillDirected(List<SQLTable> relations, List<SQLForeignKeyConstraint> constraints) {        
        // Fill the Nodes of the Graph.
        for (SQLTable relation: relations) {
            this.addNode(relation);
            this.nameToRelationMap.put(relation.getName(), relation);
        }

        // Loop through the constraints to set the edges.
        for (SQLForeignKeyConstraint constraint : constraints) {            
            SQLTable fkTableNode = this.nameToRelationMap.get(constraint.getForeignKeyColumn().getTable().getName());
            SQLTable pkTableNode = this.nameToRelationMap.get(constraint.getPrimaryKeyColumn().getTable().getName());

            // An componentRelation is not on the tableToNodeMap cause they dont correspond to
            // an ORMNode. So start node or end node will be null. Just skip this constraint.
            if (fkTableNode == null || pkTableNode == null) continue;

            // Update the relation to constraints mapping.
            this.updateRelationToConstraintsMap(fkTableNode, constraint);
            this.updateRelationToConstraintsMap(pkTableNode, constraint);
                        
            // Add the connection in the database.
            super.addDirEdge(pkTableNode, fkTableNode);
        }
    }

    // Fills the graph.
    public void fillUnDirected(List<SQLTable> relations, List<SQLForeignKeyConstraint> constraints) {        
        // Fill the Nodes of the Graph.
        for (SQLTable relation: relations) {
            this.addNode(relation);
            this.nameToRelationMap.put(relation.getName(), relation);
        }

        // Loop through the constraints to set the edges.
        for (SQLForeignKeyConstraint constraint : constraints) {            
            SQLTable fkTableNode = this.nameToRelationMap.get(constraint.getForeignKeyColumn().getTable().getName());
            SQLTable pkTableNode = this.nameToRelationMap.get(constraint.getPrimaryKeyColumn().getTable().getName());

            // An componentRelation is not on the tableToNodeMap cause they dont correspond to
            // an ORMNode. So start node or end node will be null. Just skip this constraint.
            if (fkTableNode == null || pkTableNode == null) continue;

            // Update the relation to constraints mapping.
            this.updateRelationToConstraintsMap(fkTableNode, constraint);
            this.updateRelationToConstraintsMap(pkTableNode, constraint);
                        
            // Add the connection in the database.
            super.addUnDirEdge(pkTableNode, fkTableNode);
        }
    }

     /**
     * Update the relation to constraint mapping of the relation referred
     * by the param relation to contain one more constrain , the parameter constraint.
     * 
     * @param relation the SQLTable.
     * @param constraint a constraint involving this table.
     */
    public void updateRelationToConstraintsMap(SQLTable relation, SQLForeignKeyConstraint constraint)  {
        // Get the Constraints so far or a new List.
        Set<SQLForeignKeyConstraint> constraintsSoFar = this.relationsConstraints.getOrDefault(
            relation.getName(),
            new HashSet<>() 
        );

        // Append the new constraint.
        constraintsSoFar.add(constraint);

        // Update or Add the new Mapping.
        this.relationsConstraints.put(
            relation.getName(),
            constraintsSoFar
        );
    }
    

    // Returns true if a directed edge between two SQLTables exists in the schema graph.
    public boolean getDirectedConnection(SQLTable from, SQLTable to) {
        return super.areDirConnected(from, to);
    }

    // Returns true if an undirected edge between two SQLTables exists in the schema graph.
    public boolean getUndirectedConnection(SQLTable x, SQLTable y) {
        return super.areUnDirConnected(x, y);
    }


     /**
     * Get 2 names of a relations in the SchemaGraph. Convert them to Nodes in the graph
     * and retrieve their distance in the graph. The distance is the total number of 
     * nodes (Relations) between them (including them) in a path connecting them.
     * 
     * @param nodeARelationName
     * @param nodeBRelationName
     * @return the distance of ORMNodes corresponding to parameters Relation Names.
     */
    public int getDistanceBetweenNodes(String nodeARelationName, String nodeBRelationName) {
        SQLTable nodeA = this.nameToRelationMap.get(nodeARelationName);
        SQLTable nodeB = this.nameToRelationMap.get(nodeBRelationName);

        // Get The Patch connecting those two nodes.        
        Graph<SQLTable, NoLabel> path = this.getPathConnecting2Nodes(nodeA, nodeB);
        
        // If there is no path connecting those 2 nodes return an MAX INT distance.
        if (path == null)
            return Integer.MAX_VALUE;
        // Else return the distance.
        else         
            return path.getVertexes().size();
    }

    /**
     * Get the columns joining left and right ORMNode. To get the columns we need to 
     * find the SQLForeignKeyConstraint between those two nodes and extract the right columns.
     * 
     * @param leftNode
     * @param rightNode
     * @param foreignKeyTable
     * @return
     */
    public Pair<SQLColumn, SQLColumn> getColumnsJoiningNodes(
        SQLTable leftNode, SQLTable rightNode, Pair<Boolean, Boolean> foreignKeyTable
    ) {
        // Find all the constraints that leftNode and rightNode involve.
        Set<SQLForeignKeyConstraint> leftNodeConstraints = this.relationsConstraints.get(leftNode.getName());
        Set<SQLForeignKeyConstraint> rightNodeConstraints = this.relationsConstraints.get(rightNode.getName());
        
        // Find the one constraint that both are involved.
        for (SQLForeignKeyConstraint leftNodeConstraint: leftNodeConstraints)
            for (SQLForeignKeyConstraint rightNodeConstraint: rightNodeConstraints)
                if (leftNodeConstraint == rightNodeConstraint)
                    return leftNodeConstraint.getColumnPairIfTablesPatricipateInConstraint(
                        leftNode, rightNode, foreignKeyTable
                    );


        return null;
    }

}
