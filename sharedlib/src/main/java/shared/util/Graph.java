package shared.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;


// This class models a generic graph data structure.
// We represent the graph with a adjacency list. The graph
// nodes contain T data that are unique.
public class Graph<V,L> {

    /** Represents an empty label */
    public static class NoLabel {}

    // Models a graph Edge.
    public class Edge {
        private V startNode;
        private V endNode;
        private L label;

        public Edge(V startNode, V endNode) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.label = null;
        }
        
        public Edge(V startNode, V endNode, L label) {
            this.startNode = startNode;
            this.endNode = endNode;
            this.label = label;
        }

        // Getters.
        public V getStartNode() { return this.startNode; }
        public V getEndNode() { return this.endNode; }
        public L getLabel() { return this.label; }

        // Returns true if it contains the parameter node.
        public boolean containsNode(V node) {
            return this.startNode.equals(node) || this.endNode.equals(node);
        }
    }


    // A list node models a node of the Adjacency List.
    class ListNode {
        V data;                               // The data Object.
        List<ListNode> outGoingConnections;   // The List of outgoing connections from this node.
        List<ListNode> inComingConnections;   // The List of incoming connections to this node.

        // Constructor.
        ListNode(V nodeData) {
            this.data = nodeData;
            this.outGoingConnections = new ArrayList<>();
            this.inComingConnections = new ArrayList<>();
        }

        // Add a node to the out going connections list        
        boolean addOutGoingConnection(ListNode node) {
            return this.outGoingConnections.add(node);
        }

        /**         
         * @return All the ingoing and outgoing connections as a set.
         */
        Set<ListNode> getAllConnections() {
            Set<ListNode> allNodes = new HashSet<>();
            allNodes.addAll(this.inComingConnections);
            allNodes.addAll(this.outGoingConnections);
            return allNodes;
        }
        
        // Add a node to the in coming connections list
        boolean addInComingConnection(ListNode node) {
            return this.inComingConnections.add(node);
        }

        // Return true if this node connects with parameter node.
        // The isConnected method checks only the outGoingConnections of
        // a node. As result we can specify the direction of the connection.
        boolean isConnected(ListNode node) {
            return this.outGoingConnections.indexOf(node) != -1;
        }

        // Return true if this node connects with parameter node.
        // This method checks only both outgoing of this and another
        // node. As result its an undirected check of the connection.
        boolean isUnDirConnected(ListNode another){
            return this.isConnected(another) || another.isConnected(this);
        }
        
        @Override
        public String toString() {
            String str = new String();

            // Print the data.
            str += this.data.toString() + "("+ this.hashCode() +")"  + " : ";

            // Print the out going connections
            for (ListNode con: this.outGoingConnections) {
                str += con.data.toString() + "("+ con.hashCode() +")"  + ", ";
            }
            str = str.substring(0, str.length() - 2) + "\n";

            // Return the string.
            return str;
        }        
    }

    private List<ListNode> adjacencyList;            // The adjacency List (The vertexes of the graph).
    private List<Edge> edgesList;                    // The edges of the graph.
    private HashMap<V, ListNode> dataToNodeMapping;  // Mapping data to nodes.

    // Public Constructor.
    public Graph() {
        this.adjacencyList = new ArrayList<>();
        this.edgesList = new ArrayList<>();
        this.dataToNodeMapping = new HashMap<>();
    }

    // Clone Vertexes from parameter Graph. Shallow cloning the nodes.
    public void cloneVertexes(Graph<V,L> graph) {
        // Clone the adjacencyList
        for (ListNode node: graph.adjacencyList) {            
            ListNode clonedNode = new ListNode(node.data);
            this.dataToNodeMapping.put(node.data, clonedNode);
            this.adjacencyList.add(clonedNode);
        }
    }
    
    /**
     * Make this graph a clone of the parameter Graph. If the actual 
     * data of the Graph will be cloned is up to the Type of T.
     * 
     * @param graphToClone graph to clone
     * @param fillDataToClonedData a maps between param graph's data and this graph's cloned data. if its not null fill it.
     */    
    public void cloneLikeGraph(Graph<V,L> graphToClone, HashMap<V, V> fillDataToClonedData) {        
        HashMap<ListNode, ListNode> oldToNewNodes = new HashMap<>();  // Map cloned nodes to nodes.
 
        // First clone all the nodes keeping the mapping between clonedNodes and this nodes.
        for (ListNode node: graphToClone.adjacencyList) {
            V clonedData = cloneData(node.data);            // Clone the node's data.
            ListNode clonedNode = new ListNode(clonedData); // Clone the node.                         

            // Update the maps.
            this.dataToNodeMapping.put(clonedData, clonedNode);
            this.adjacencyList.add(clonedNode);
            oldToNewNodes.put(node, clonedNode);

            // Update the Data to Cloned Data parameter Map.
            if (fillDataToClonedData != null)
                fillDataToClonedData.put(node.data, clonedData);
        }

        // Traverse the AdjacencyList again: This time for every node in the graphToClone 
        // clone it's outGoingConnections in the to the clone's Nodes.
        for (ListNode node: graphToClone.adjacencyList) {
            for(ListNode connectedNode: node.outGoingConnections) {
                this.addDirEdge(
                    oldToNewNodes.get(node).data ,  
                    oldToNewNodes.get(connectedNode).data
                );
            }
        }        
    }
    

    /** 
     * Return the Clone of the data if it is possible. Else 
     * return the same object.
     * 
     * @param data the data to clone.
     * @return data.clone() or data depending on the type of data.
     */
    @SuppressWarnings("unchecked")
    private V cloneData(V data) {
        V clonedData = null;
        try {
            clonedData = (V) data.getClass().getMethod("clone").invoke(data);
        } catch (Exception e) {
            clonedData = data;
        }
        return clonedData;
    }


    /**     
     * @param nodeData the node whose neighbors we are going to return.
     * @return The neighbors of param nodeData
     */
    public List<V> getNodesNeighbors(V nodeData) {
        List<V> neighbors = new ArrayList<>();
        for (ListNode nNode: this.dataToNodeMapping.get(nodeData).outGoingConnections)
            neighbors.add(nNode.data);
        return neighbors;
    }


    /**
     * Return the Edge between two Neighboring Nodes.
     * The direction of the edge must be from startNode to endNode.
     * 
     * @param startNode The 
     */
    public boolean areDirConnected(V startNodeData, V endNodeData) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) return false;

        return startNode.isConnected(endNode);
    }

    /**
     * Return the Edge between two Neighboring Nodes.
     * The direction of the edge must be from startNode to endNode.
     * 
     * @param startNode The 
     */
    public boolean areUnDirConnected(V startNodeData, V endNodeData) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) return false;

        return startNode.isConnected(endNode) || endNode.isConnected(startNode);
    }


    // Add a new node whit data T.
    public boolean addNode(V nodeData) {
        // If the node exists return.
        if (this.getNode(nodeData) != null) return false;

        // Create a new ListNode.
        ListNode newNode = new ListNode(nodeData);

        // Keep the data to node mapping.
        this.dataToNodeMapping.put(nodeData, newNode);
        
        // Add the node in the list.
        return this.adjacencyList.add(newNode);
    }

    // Return the node with data equal to nodeData.
    // If not found return null.
    public ListNode getNode(V nodeData) {        
        return this.dataToNodeMapping.get(nodeData);
    }

    /**
     * Return all the nodes corresponding to the nodeDataCollection.
     * 
     * @param nodeDataCollection
     * @return
     */
    public List<ListNode> getAllNodes(Collection<V> nodeDataCollection) {
        List<ListNode> allNodes = new ArrayList<>();
        for (V nodeData: nodeDataCollection)
            allNodes.add( this.getNode(nodeData) );
        return allNodes;
    }


    // Add a connection between two nodes.
    public boolean addDirEdge(V startNodeData, V endNodeData) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) return false;

        // Create an edge and add it to the edge list.
        this.edgesList.add(new Edge(startNodeData, endNodeData));
        
        // Connect The nodes of the adjacency List.
        return startNode.addOutGoingConnection(endNode) &&
               endNode.addInComingConnection(startNode);
    }

    // Add a connection between two nodes.
    public boolean addUnDirEdge(V startNodeData, V endNodeData) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) {            
            return false;
        }

        // Create two edges and add them to the edge list.
        this.edgesList.add(new Edge(startNodeData, endNodeData));
        this.edgesList.add(new Edge(endNodeData, startNodeData));

        // Add a undirected Edge between startNode and endNode.
        return startNode.addOutGoingConnection(endNode) && endNode.addInComingConnection(startNode) &&
               endNode.addOutGoingConnection(startNode) && startNode.addInComingConnection(endNode);
    }

     // Add a connection between two nodes.
     public boolean addDirEdge(V startNodeData, V endNodeData, L label) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) return false;

        // Create an edge and add it to the edge list.
        this.edgesList.add(new Edge(startNodeData, endNodeData, label));
        
        // Connect The nodes of the adjacency List.
        return startNode.addOutGoingConnection(endNode) &&
               endNode.addInComingConnection(startNode);
    }

    // Add a connection between two nodes.
    public boolean addUnDirEdge(V startNodeData, V endNodeData, L label) {
        ListNode startNode = this.getNode(startNodeData);
        ListNode endNode = this.getNode(endNodeData);

        // Check if nodes dont exist.
        if (startNode == null || endNode == null) {            
            return false;
        }

        // Create two edges and add them to the edge list.
        this.edgesList.add(new Edge(startNodeData, endNodeData, label));
        this.edgesList.add(new Edge(endNodeData, startNodeData, label));

        // Add a undirected Edge between startNode and endNode.
        return startNode.addOutGoingConnection(endNode) && endNode.addInComingConnection(startNode) &&
               endNode.addOutGoingConnection(startNode) && startNode.addInComingConnection(endNode);
    }


    // Returns a list of all the data contained in the graph.
    public List<V> getVertexes() {
        List<V> dataList = new ArrayList<>();
        for(ListNode node: this.adjacencyList)
            dataList.add(node.data);
        return dataList;
    }


    public List<Edge> getEdges() {        
        return edgesList;
    }

    /**
     * <pre>
     * The radius node of a graph is the node with the smallest centric distance.
     * The centric distance of a node 'u' in a Graph 'P' is defined : 
     *      centric(u,P) = max dist(u,v,P) | ForEach v in P.
     * 
     * @return The radius node of this Graph.
     * </pre>
     */
    public V getRadius() {
        Integer minDistance = Integer.MAX_VALUE;
        V minDistNode = null;

        // Loop all the nodes in this graph.
        for (V node: this.getVertexes()) {
            // Keep the one with the minimum centric distance
            Integer dist = this.getCentricDistance(node);
            if (dist < minDistance) {
                minDistance = dist;
                minDistNode = node;
            }
        }


        return minDistNode;
    }


    /**
     * <pre>
     * The centric distance of a node 'u' in a Graph 'P' is defined : 
     *      centric(u,P) = max dist(u,v,P) | ForEach v in P.
     * 
     * @param node
     * @return
     * </pre>
     */
    public Integer getCentricDistance(V node) {
        Integer maxDistance = -1;

        // Loop all the nodes in this graph
        for (V anotherNode: this.getVertexes()) {
            if (node == anotherNode) continue;
            Integer dist = this.getPathDistance(node, anotherNode);
            if (dist > maxDistance)
                maxDistance = dist;
        }

        return maxDistance;
    }



    /**
     * Calculates the distance between two nodes in the graph. 
     * This is archived by finding the path between them an 
     * measuring the number of edges in the path. In this case
     * we just measure the number of nodes in the path - 1.
     * 
     * @param startDataNode
     * @param endDataNode
     * @return returns the distance of the above nodes.
     */
    public int getPathDistance(V startDataNode, V endDataNode) {
        Graph<V,L> path = this.getPathConnecting2Nodes(startDataNode, endDataNode);

        // If there is no path connecting them return MAX distance.
        if (path == null)
            return Integer.MAX_VALUE;
        // The distance can be measured as the number of nodes in the path minus 1. (The edges)
        else 
            return path.adjacencyList.size() - 1;
    }   


    // Returns a the path connecting two DataNodes. The path is returned in graph format.
    public Graph<V,L> getPathConnecting2Nodes(V startDataNode, V endDataNode) {
        // Create a List containing those two nodes.
        Set<V> containedData = new HashSet<>();
        containedData.add(startDataNode);
        containedData.add(endDataNode);

        // And Call the subGraph function to find the path.
        return this.subGraph(containedData);
    }    


    // Check nodes before sub graph
    public boolean checkNodesForSubGraph(Set<V> containedData) {
        // Each node must be in the graph and each node must have
        // at least one incoming edge or outgoing connection.
        for (V nodeData: containedData) {
            ListNode listNode = this.getNode(nodeData);
            if (listNode == null) return false;
            else if (
                listNode.inComingConnections.isEmpty() &&
                listNode.outGoingConnections.isEmpty()
            )
                return false;
        }

        return true;
    }


    /**
     * Returns a subGraph of the this {@link Graph} containing the nodes passed as a parameter.
     * 
     * @param containedData
     * @return A sub-{@link Graph}
     */
    public Graph<V,L> subGraph(Set<V> containedData) {                
        Graph<V,L> subGraph = new Graph<V,L>();               // The subGraph.        
        Set<ListNode> nodesToExpand = new HashSet<>();        // A List holding the ListNodes of the GRAPH that we will expand.
        List<ListNode> finalNodes = new ArrayList<>();        // A final node is a node with no outGoing Edges. This list stores final SUB-GRAPH nodes.

        // =============================================================================================
        // Keep in mind that this.Nodes are different than subGraph.Nodes but contain the same NodeData.
        // =============================================================================================

        // Check the special case where containedData contains only one 
        // node, and return the graph containing that node only.
        if (containedData.size() == 1) {
            subGraph.addNode(containedData.iterator().next());
            return subGraph;
        }

        // Check nodes before starting the subGraph.
        if (containedData.isEmpty()) return subGraph;            
        if (checkNodesForSubGraph(containedData) == false)
            return null;
        
        // Initialize the subGraph with all the object form the containedData set.
        for (V newNode: containedData)
            subGraph.addNode(newNode);

        // Initialize the other variables.
        nodesToExpand.addAll(this.getAllNodes(containedData));
        finalNodes.addAll(subGraph.getAllNodes(containedData));

        // Loop until the subGraph achieves the right amount of connections.
        // Starting the subGraph contains | containedData | disconnected parts.
        // 
        Boolean isConnected = false;
        while (!isConnected)
            isConnected = subGraph.expandAllByOne(nodesToExpand, finalNodes);
        

        // Prune unwanted paths that dont contain containedData.
        subGraph.pruneUnwantedPaths(containedData, finalNodes);        

        return subGraph;
    }

    /**
     * 
     * @param nodesToExpand
     * @param finalNodes
     * @return The number of connections achieved while disconnected parts of the subGraph Expanded.
     */
    private boolean expandAllByOne(Set<ListNode> nodesToExpand, List<ListNode> finalNodes) {
        Set<ListNode> newNodesToExpand = new HashSet<>();

        // ================================================================================================
        // Keep in mind that nodesToExpand are different than subGraph.Nodes but contain the same NodeData.
        // NodesToExpand are from the initial graph, not the sugGraph.
        // ================================================================================================
        
        
        // Loop all the graph nodes corresponding to this subGraph's Nodes.
        for (ListNode node: nodesToExpand) {            
            // Get the node corresponding to this subGraphs node, which has the same data as the Graph node.
            ListNode thisSubGraphNode = this.getNode(node.data);

            // Loop all the neighboring nodes and add them to as neighbors of thisSubGraphNode.
            for (ListNode neighboringNode: node.getAllConnections()) {
                ListNode thisNeighboringNode = this.getNode(neighboringNode.data);

                // If the neighboringNode is not already in this subGraph add it and link it with thisSubGraphNode.
                if (thisNeighboringNode == null) {
                    this.addNode(neighboringNode.data);   // Add the node.
                    thisNeighboringNode = this.getNode(neighboringNode.data);

                    // Keep the way this graph is connected (dir or unDir)
                    if (node.isConnected(neighboringNode))
                        this.addDirEdge(thisSubGraphNode.data, neighboringNode.data);
                    if (neighboringNode.isConnected(node))
                        this.addDirEdge(neighboringNode.data, thisSubGraphNode.data);
                    

                    // ThisSubGraphNode is no longer a final node. But the new 
                    // node with data: neighboringNode.data added above is a final node.
                    finalNodes.remove(thisSubGraphNode);
                    finalNodes.add(thisNeighboringNode);

                    // Expand this node on the next call of this function.
                    newNodesToExpand.add(neighboringNode);                                        
                }
                // Else thisNeighboringNode exists but its not connected with thisSubGraphNode 
                // then connect it and remove both from finalNodes. 
                // Check if the whole graph is connected and if yes return.
                else if (!thisNeighboringNode.isUnDirConnected(thisSubGraphNode)) {                    
                    if (node.isConnected(neighboringNode))
                        this.addDirEdge(thisSubGraphNode.data, thisNeighboringNode.data);
                    if (neighboringNode.isConnected(node))
                        this.addDirEdge(thisNeighboringNode.data, thisSubGraphNode.data);

                    finalNodes.removeAll(Arrays.asList(thisNeighboringNode, thisSubGraphNode));
                    if (this.isConnected(thisNeighboringNode)) return true;
                }
            }            
        }

        // Replace the nodesToExpand with the newNodesToExpand for the next call.
        nodesToExpand.clear(); 
        nodesToExpand.addAll(newNodesToExpand);
        return false;    
    }

    /**
     * Traverses all the graph and checks if it can visit all its nodes.
     * @param OptionalStartPoint  Its an optional start point.
     * @return
     */
    private boolean isConnected(ListNode optionalStartPoint) {
        if (this.adjacencyList.isEmpty()) return true;

        // Pick a starting point if its null.
        if (optionalStartPoint == null)
            optionalStartPoint = this.adjacencyList.get(0);

        // Start traversing the graph
        HashSet<ListNode> visitedNodes = new HashSet<>();
        Queue<ListNode> queue = new LinkedList<>();

        // Insert the starting point
        queue.add(optionalStartPoint);
        visitedNodes.add(optionalStartPoint);
        

        // Start traversing
        while (!queue.isEmpty()) {
            ListNode node = queue.poll();

            // Add node's children in the queue
            for (ListNode child: node.getAllConnections()) {

                // Skip visited nodes
                if (visitedNodes.contains(child))
                    continue;

                // Assign node as visited.
                visitedNodes.add(child);
                queue.add(child);
            }            
        }

        // If number of nodes in the graph are not equal to 
        // visited nodes then return false.
        if (this.adjacencyList.size() != visitedNodes.size())
            return false;
        else 
            return true;
    }

    // Prune paths in the graph that dont contain any of the wantedData in the
    // parameter list.
    // Start from the finalNodes of the graph with no outgoing edges and prune all nodes that 
    // dont contain an object from the list, and are not used as intermediate nodes connecting such nodes.
    private void pruneUnwantedPaths(Set<V> wantedData, Collection<ListNode> finalNodes) {
        // Loop all the final nodes and start pruning from these nodes.
        for(ListNode finalNode: finalNodes) {
            this.recPruning(finalNode, wantedData);
        }
    }

    // Recursive pruning of nodes with no outgoing edges. In case the node's data match 
    // one of the wantedData, leave the node intact. 
    private void recPruning(ListNode nodeToPrune, Set<V> wantedData) {
        // If the data of the nodeToPrune is a wantedObject dont prune that node.
        if (wantedData.contains(nodeToPrune.data)) return;

        // If the nodes is used to connect 2 other nodes then dont prune it.
        if (nodeToPrune.getAllConnections().size() >= 2) return;

        // Else prune the node and call the rec function for all nodes connected with this node.
        this.removeNode(nodeToPrune.data);
        for (ListNode neighboringNode: nodeToPrune.inComingConnections) {
            this.recPruning(neighboringNode, wantedData);
        }        
    }

    
    // Removes a node with data like parameter NodeData, along with edges that start or end at this node.
    private boolean removeNode(V nodeData) {
        // First find the node.
        ListNode nodeToRemove = this.getNode(nodeData);
        if (nodeToRemove == null) return false;

        // Then Follow the incoming edges and remove those edges from the nodes
        // connecting with nodeToRemove node.
        for (ListNode neighboringNode: nodeToRemove.inComingConnections)
            neighboringNode.outGoingConnections.remove(nodeToRemove);

        // Then Follow the outgoing edges and remove those edges from the nodes
        // connecting with nodeToRemove node.
        for (ListNode neighboringNode: nodeToRemove.outGoingConnections)
            neighboringNode.inComingConnections.remove(nodeToRemove);
            
        // Remove all edges connected with this node from list.
        List<Edge> edgesToRemove = new ArrayList<>();
        for (Edge edge: this.edgesList) 
            if (edge.containsNode(nodeData))
                edgesToRemove.add(edge);
        this.edgesList.removeAll(edgesToRemove);

        // Then remove the node.        
        this.dataToNodeMapping.remove(nodeData);
        return this.adjacencyList.remove(nodeToRemove);
    }



    @Override
    public String toString() {
        String str = "_Graph_\n";

        // Print each node of the list.
        for (Graph<V,L>.ListNode node : this.adjacencyList) {
           str += node.toString();
        }

        // Return the str.
        return str;
    }

}

