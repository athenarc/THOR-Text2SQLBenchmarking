package shared.database.model.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import shared.database.model.SQLColumn;
import shared.database.model.SQLForeignKeyConstraint;
import shared.database.model.SQLTable;
import shared.util.Pair;

public class ORMNode {

    // The 3 different types od an ORMGraphNode.
    public enum NodeType { Object, Relationship, Mixed };
    
    private List<NodeType> type;  // The node Types.
    private SQLTable relation;    // The relation Connected with the node.

    // Optional field for Mixed and Object nodes only.
    private HashMap<SQLTable, SQLForeignKeyConstraint> componentRelations;

    // Public Constructors.
    public ORMNode(NodeType type, SQLTable relation) {
        this.type = new ArrayList<>();
        this.type.add(type);
        this.relation = relation;
        this.componentRelations = new HashMap<>();
    }

    public ORMNode(NodeType typeA, NodeType typeB, SQLTable relation) {
        this.type = new ArrayList<>();
        this.type.add(typeA); this.type.add(typeB);
        this.relation = relation;
        this.componentRelations = new HashMap<>();
    }



    // Getters.
    public SQLTable getRelation() { return relation; }    
    public List<SQLTable> getComponentRelations() { return new ArrayList<>(componentRelations.keySet()); }


    /**    
     * @param relName the name of the component Relation.
     * @return Return the Component Relation with name relName and the Constraint 
     * connecting this node's Object/Mixed relation with the Component Relation.
     */
    public Pair<SQLTable, SQLForeignKeyConstraint> getComponentRelationByName(String relName) {        
        for (Map.Entry<SQLTable, SQLForeignKeyConstraint> entry: this.componentRelations.entrySet())
            if (entry.getKey().getName().equals(relName))
                return new Pair<>(entry.getKey(), entry.getValue());        
        return null;
    }


    // Add a componentRelation.
    public void addComponentRelation(SQLTable compRelation, SQLForeignKeyConstraint constraint) {
        this.componentRelations.put(compRelation, constraint);
    }


    /**
     * Find the Attribute with name attributeName connected with this node. The attribute can 
     * be an SQLColumn of the relation connected with this ORMNode or an SQLColumn of the 
     * component Relations assigned to this node by the ORMSchemaGraph creation Process.
     * 
     * @param attributeName The name of the attribute we seek to return.
     * @return The SQLColumn corresponding to an Attribute in the Database with the above name.
     */
    public SQLColumn getAttributeByName(String attributeName) {
        // First look on the Relation's attributes corresponding to this node.
        SQLColumn column = this.relation.getColumnByName(attributeName);

        // If the column is null then look in the component Relations of this node.
        if (column == null)            
            for (Map.Entry<SQLTable, SQLForeignKeyConstraint> entry: this.componentRelations.entrySet())
                if ( (column = entry.getKey().getColumnByName(attributeName)) != null )
                    break;
            
                
        // Return the column
        return column;
    }


    /**
     * @return A boolean indicating if this node is a node of Type: Relationship.
     */
    public boolean isRelationshipNode() {
        return this.type.indexOf(NodeType.Relationship) != -1;
    }

    /**
     * @return A boolean indicating if this node is a node of Type: Object.
     */
    public boolean isObjectNode() {
        return this.type.indexOf(NodeType.Object) != -1;
    }

    /**
     * @return A boolean indicating if this node is a node of Type: Mixed.
     */
    public boolean isMixedNode() {
        return this.type.indexOf(NodeType.Mixed) != -1;
    }

    
    @Override
    public String toString() {
        return this.relation.getName() + "(" + this.type + ")";
    }
    

}