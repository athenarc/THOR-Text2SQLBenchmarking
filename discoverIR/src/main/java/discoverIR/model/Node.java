package discoverIR.model;

import java.util.ArrayList;
import java.util.List;

// The node of the network.
public class Node {

    TupleSet tupleSet;
    Node parent;
    List<Node> children;

    public Node() {
        this.tupleSet = null;
        this.parent = null;
        this.children = new ArrayList<Node>();
    }

    public Node(TupleSet tupleSet) {
        this.tupleSet = tupleSet;
        this.parent = null;
        this.children = new ArrayList<Node>();
    }

    public Node(TupleSet tupleSet, Node parent) {
        this.tupleSet = tupleSet;
        this.parent = parent;
        this.children = new ArrayList<Node>();
    }

    public Node(Node parent) {
        this.tupleSet = null;
        this.parent = parent;
        this.children = new ArrayList<Node>();
    }

    // Returns the tuple Set.
    public TupleSet getTupleSet() {
        return tupleSet;
    }
    
    // Sets a new tupleSet.
    public void setTupleSet(TupleSet tupleSet) {
        this.tupleSet = tupleSet;
    }

    // Returns the node's children.
    public List<Node> getChildren() {
        return children;
    }

    // Add a node as child.
    public void addChild(Node child) {
        this.children.add(child);
    }

    // Returns true if a node has a parent node.
    public boolean hasParent() {
        return this.parent != null;
    }

    // Returns true if the node is a leaf (has no children).
    public boolean isLeaf() {
        return this.children.size() == 0;
    }

    // Returns true if the node was a leaf before being expanded.
    // We call this function on nodes that were just expanded
    // in order to keep count of the total leaf nodes of the network.
    public boolean wasLeaf() {
        return this.children.size() == 1;
    }

    // Shallow copies a given node's contents into the current node's contents.
    public void shallowCopy(Node src) {
        this.tupleSet = src.tupleSet;
        this.parent = src.parent;
        this.children = src.children;
    }

    // Returns a node's joinable expression's abbreviation. In this case it is a tuple set's abbreviation.
    public String getJoinableExpressionsAbbreviation() {
        return this.tupleSet.toAbbreviation();
    }

}