package discover.model;

import java.util.List;
import java.util.ArrayList;


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

    // Returns the node's children.
    public List<Node> getChildren() {
        return children;
    }

    public void shallowCopy(Node srcNode) {
        this.tupleSet = srcNode.tupleSet;
        this.parent = srcNode.parent;
        this.children = srcNode.children;
    }

    // Returns a node's joinable expression. In this case it is a tuple set.
    public JoinableExpression getJoinableExpression() {
        return this.tupleSet;
    }

    // Returns a node's joinable expression's abbreviation. In this case it is a tuple set's abbreviation.
    public String getJoinableExpressionsAbbreviation() {
        return this.tupleSet.toAbbreviation();
    }

    // Returns a node's joinable expression's to string. In this case it is a tuple set's abbreviation.
    public String getJoinableExpressionsToStr() {
        return this.tupleSet.toString();
    }

    // Return a Father Child pair if the node's and one of each
    // children's Joinable Expressions match the ones in the
    // Joinable Pair.
    public Node containsExpressionsOfJoinablePair(JoinablePair pair) {
        Node childNode = null;

        // Check if the node contains one of the pairs expressions
        // and if so, find his child containing the other expression.        
        if (this.containsExpression(pair.getLeft())) {
            for (Node child: this.children) {
                if (child.containsExpression(pair.getRight())) {
                    childNode = child;
                    break;
                }
            }
        }
        else if (this.containsExpression(pair.getRight())) {
            for (Node child: this.children) {
                if (child.containsExpression(pair.getLeft())) {
                    childNode = child;
                    break;
                }
            }
        }

        return childNode;
    }

    // Return true if node contains a JoinableExpression. In this case a TupleSet.
    public boolean containsExpression(JoinableExpression expression) {
        return this.tupleSet.equals(expression);
    }

    public TupleSet getTupleSet() {
        return this.tupleSet;
    }

}