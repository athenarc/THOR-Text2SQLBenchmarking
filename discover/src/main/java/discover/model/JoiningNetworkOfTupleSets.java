package discover.model;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import discover.model.execution.IntermediateResultAssignment;
import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.util.Pair;
import shared.util.PrintingUtils;

import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedList;

// The node of the network containing an assignment.
// This node stores a pair of joinable Tuple Sets.
// Two normal Nodes or one Normal and on IRNode
// are replaced by an IntermediateResultNode
// inside the JNTS.
class IntermediateResultNode extends Node {

    JoinablePair intermediateResult; // A pair of joinable expressions.

    public IntermediateResultNode() {
        super();
    }

    public IntermediateResultNode(JoinablePair pair) {
        super();
        this.intermediateResult = pair;
    }

    public void updateIntermediateNode(Node nodeToMergeWith, Node childToMergeWith, JoiningNetworkOfTupleSets jnts) {
        // Update IntermediateResultNode's parent.
        this.parent = nodeToMergeWith.parent;

        // Check if nodeToMergeWith was the root and make it the new root.
        if (nodeToMergeWith.parent == null) {
            jnts.setRoot(this);
        }
        // If not Update node's parent to have this as its child.
        else {
            Node parent = nodeToMergeWith.parent;
            parent.children.remove(nodeToMergeWith);
            parent.children.add(this);
        }

        // Remove nodeToMergeWith from node's children
        nodeToMergeWith.children.remove(childToMergeWith);

        // Make All nodeToMergeWith and childToMergeWith
        // children to have this as parent.
        // And add them as this children.
        for (Node child : nodeToMergeWith.children) {
            this.children.add(child);
            child.parent = this;
        }
        for (Node child : childToMergeWith.children) {
            this.children.add(child);
            child.parent = this;
        }
    }

    // Return Joinable Expression's Abbreviation. In this case Joinable Pair's
    // Abbreviation.
    @Override
    public String getJoinableExpressionsAbbreviation() {
        return this.intermediateResult.toAbbreviation();
    }

    // Return Joinable Expression's to String. In this case Joinable Pair's
    // Abbreviation.
    @Override
    public String getJoinableExpressionsToStr() {
        return this.intermediateResult.toString();
    }

    // Get node's Joinable Expression. In this case its a JoinablePair.
    @Override
    public JoinableExpression getJoinableExpression() {
        return this.intermediateResult;
    }

    // Return true if node contains a JoinableExpression. In this case a
    // JoinablePair.
    @Override
    public boolean containsExpression(JoinableExpression expression) {
        return this.intermediateResult.equals(expression);
    }

    @Override
    public Node containsExpressionsOfJoinablePair(JoinablePair pair) {
        return super.containsExpressionsOfJoinablePair(pair);
    }
}

// A joining network of tuple sets J is a tree of tuple sets
// where for each pair of adjacent tuple sets Ri^K, Rj^M in J
// there is an an edge between the tables Ri Rj in the schema graph.
public class JoiningNetworkOfTupleSets implements JoinableExpression {

    private Node root; // The root of the network.
    private int size; // The size of the network (number of joins).
    private Set<TupleSet> networkTupleSets; // All the tuple sets in the network.

    private int freeTupleSetLeaves; // The number of free tuple sets as leaves in the network.
    private int totalLeaves; // The total number of leaves in the network.
    private boolean violatesPruningCondition; // Indicates whether a JNTS violates the pruning condition.

    // Maps every keyword contained in the network with its number of occurrences.
    private Map<String, Integer> keywordOccurrences;

    // Copy constructor (only copies the variables).
    public JoiningNetworkOfTupleSets(JoiningNetworkOfTupleSets src) {
        this.root = null;
        this.size = src.getSize();
        this.networkTupleSets = new HashSet<TupleSet>();
        copySet(src.getTupleSets(), this.networkTupleSets);

        this.freeTupleSetLeaves = src.getFreeTupleSetLeaves();
        this.totalLeaves = src.getTotalLeaves();
        this.violatesPruningCondition = src.getViolatesPruningCondition();

        this.keywordOccurrences = new HashMap<String, Integer>();
        copyMap(src.getKeywordOccurrences(), this.keywordOccurrences);
    }

    // Creates a new JNTS with the given tuple set as a root node.
    public JoiningNetworkOfTupleSets(TupleSet tupleSet) {
        this.root = new Node(tupleSet);
        this.size = 0;
        this.networkTupleSets = new HashSet<TupleSet>();
        this.networkTupleSets.add(tupleSet);

        this.freeTupleSetLeaves = 0;
        this.totalLeaves = 1;
        this.violatesPruningCondition = false;

        // Add the keywords of the tuple set.
        // A single tuple set does not contain duplicate keywords so we
        // just put the keywords as keys in the hashTable.
        this.keywordOccurrences = new HashMap<String, Integer>();
        for (String keyword : tupleSet.getKeywords()) {
            this.keywordOccurrences.put(keyword, 1);
        }
    }

    // Getters and Setters.
    public void setRoot(Node root) {
        this.root = root;
    }

    public Node getRoot() {
        return root;
    }

    public JoinableExpression getRootsJoinableExpression() {
        return root.getJoinableExpression();
    }

    public TupleSet getRootsTupleSet() {
        if (root instanceof IntermediateResultNode)
            return null;
        return root.tupleSet;
    }

    public int getSize() {
        return this.size;
    }

    public Set<TupleSet> getTupleSets() {
        return this.networkTupleSets;
    }

    public int getFreeTupleSetLeaves() {
        return this.freeTupleSetLeaves;
    }

    public int getTotalLeaves() {
        return this.totalLeaves;
    }

    public boolean getViolatesPruningCondition() {
        return this.violatesPruningCondition;
    }

    public Map<String, Integer> getKeywordOccurrences() {
        return this.keywordOccurrences;
    }

    // Returns true if a candidate network J violates the pruning condition:
    // A candidate network J does not contain a subtree of the form R^K - S^L - R^M,
    // where R and S are relations and the schema graph has an edge R -> S.
    public boolean violatesPruningCondition() {
        return this.violatesPruningCondition;
    }

    // Returns true if a candidate network J satisfies the acceptance conditions:
    // 1. The tuple sets of J contain all keywords of the query, i.e., keywords(J) =
    // {k1, ..., km}.
    // 2. J does not contain any free tuple sets as leaves.
    public boolean satisfiesAcceptanceConditions(List<String> keywords) {
        return ((keywords.size() == this.keywordOccurrences.size()) && (this.freeTupleSetLeaves == 0));
    }

    // A network cannot have more than k-m leaves, where k is the number of all
    // keywords in the query
    // and m is the number of keywords already in the tuple sets of the network.
    // Returns true if the network satisfies the above condition.
    public boolean satisfiesLeavesCondition(int allKeywords) {
        int networkKeywords = this.keywordOccurrences.size();
        return (allKeywords != networkKeywords) ? (this.totalLeaves <= allKeywords - networkKeywords) : true;
    }

    // Performs a breadth first traversal of the tree and returns a list
    // of the adjacent tuple sets of every node (of the tree) in the tuple set
    // graph.
    // If a node does not have any adjacent tuple sets it is not added to the list.
    public List<AdjacentTupleSets> getAdjacentTupleSets(TupleSetGraph tupleSetGraph) {
        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.
        List<AdjacentTupleSets> adjacent = new ArrayList<AdjacentTupleSets>();

        queue.add(this.root);
        while (!queue.isEmpty()) {
            Node node = queue.remove();

            // Get the adjacent tuple sets of the current node.
            AdjacentTupleSets adjacentOfCurrentNode = tupleSetGraph.getAdjacentTupleSets(node.tupleSet);

            if (adjacentOfCurrentNode != null) {
                // Find the tuple sets that are already in the tree to remove.
                List<TupleSet> tupleSetsToRemove = new ArrayList<TupleSet>();
                for (TupleSet tupleSet : adjacentOfCurrentNode.getAdjacentTupleSets()) {
                    if (this.networkTupleSets.contains(tupleSet)) {
                        tupleSetsToRemove.add(tupleSet);
                    }
                }

                if (!tupleSetsToRemove.isEmpty()) {
                    adjacentOfCurrentNode.removeTupleSets(tupleSetsToRemove);
                }
                adjacent.add(adjacentOfCurrentNode);
            }

            // Add the children of the node to the queue.
            for (Node child : node.children) {
                queue.add(child);
            }
        }

        return adjacent;
    }

    // Returns true if a candidate network J and a given tuple set Ri^K satisfies
    // the expansion rule.
    // Performs a breadth first traversal to apply the rule for every tuple set
    // Rj^M.
    // The given tuple set is the Ri^K to be checked for insertion in the network.
    public boolean checkExpansionRule(TupleSet tupleSet) {
        // If K = {} (thus, Ri^K is a free tuple set) it is accepted.
        if (tupleSet instanceof FreeTupleSet)
            return true;

        // If Ri^K's keywords are already in the tree, its not accepted.
        int keywordsExistInJNTS = 0;
        for (String keyword : tupleSet.getKeywords()) {
            keywordsExistInJNTS += (this.keywordOccurrences.get(keyword) != null) ? 1 : 0;
        }
        if (keywordsExistInJNTS == tupleSet.getKeywords().size())
            return false;

        // This hashmap will keep the updated keyword occurrences of the
        // (C U Ri^K) network, where C is the current network.
        // The first step is to copy the pairs of the keywordOccurrences into this
        // hashmap.
        Map<String, Integer> expandedKeywordOccurrences = new HashMap<String, Integer>();
        copyMap(this.keywordOccurrences, expandedKeywordOccurrences);
        // System.out.println("keywords(C): " + this.keywordOccurrences);

        // Add the keywords of tuple set Ri^K.
        // If the keyword exists in the hashmap we just increase its occurrence value by
        // 1.
        // Otherwise, we add the new keyword with an occurrence value of 1.
        for (String keyword : tupleSet.getKeywords()) {
            expandedKeywordOccurrences.put(keyword, expandedKeywordOccurrences.getOrDefault(keyword, 0) + 1);
        }
        // System.out.println("keywords(C U " + tupleSet.toAbbreviation() + "): " +
        // expandedKeywordOccurrences);

        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.

        queue.add(this.root);
        while (!queue.isEmpty()) {
            Node node = queue.remove(); // The tuple set of this node is the Rj^M.

            // Add the children of the node to the queue.
            for (Node child : node.children) {
                queue.add(child);
            }

            // Free tuple sets do not contribute any keywords, so skip them.
            if (node.tupleSet instanceof FreeTupleSet)
                continue;

            // This hashmap will keep the updated keyword occurrences of the ((C U Ri^K) -
            // Rj^M) network.
            Map<String, Integer> newExpandedKeywordOccurrences = new HashMap<String, Integer>();
            copyMap(expandedKeywordOccurrences, newExpandedKeywordOccurrences);

            // Remove the keywords of Rj^M.
            for (String keyword : node.tupleSet.getKeywords()) {
                newExpandedKeywordOccurrences.put(keyword, newExpandedKeywordOccurrences.get(keyword) - 1);
                if (newExpandedKeywordOccurrences.get(keyword) == 0) {
                    newExpandedKeywordOccurrences.remove(keyword);
                }
            }

            // System.out.println("keywords((C U " + tupleSet.toAbbreviation() + ") - " +
            // node.tupleSet.toAbbreviation() + "): " + newExpandedKeywordOccurrences);

            // Compare the two sets of keys the hashmaps contain.
            if (expandedKeywordOccurrences.keySet().equals(newExpandedKeywordOccurrences.keySet())) {
                return false;
            }
        }

        return true;
    }

    // Performs a deep copy operation between to map objects.
    // The first argument is the source, and the second is the destination.
    private void copyMap(Map<String, Integer> src, Map<String, Integer> dest) {
        for (Map.Entry<String, Integer> entry : src.entrySet()) {
            dest.put(entry.getKey(), entry.getValue());
        }
    }

    // Performs a deep copy operation between to set objects.
    // The first argument is the source, and the second is the destination.
    private void copySet(Set<TupleSet> src, Set<TupleSet> dest) {
        for (TupleSet tupleSet : src) {
            dest.add(tupleSet);
        }
    }

    // Creates a new JNTS by attaching the 'adjacent' tuple set to the current
    // network.
    // The depth variable is needed to differentiate between multiple occurrences
    // of the same tuple set in the network, and treat them independently.
    // It creates a deep copy of the current network while expanding it.
    public JoiningNetworkOfTupleSets expand(TupleSet tupleSet, TupleSet adjacent, int depth,
            TupleSetGraph tupleSetGraph) {
        Node expandedNode = new Node(); // The node that the adjacent tupleSet will be attached to.

        // Create a deep copy of the current network and expand it with the adjacent
        // tuple set.
        JoiningNetworkOfTupleSets jnts = new JoiningNetworkOfTupleSets(this);
        jnts.setRoot(copyNetwork(tupleSet, adjacent, 0, depth, this.root, null, expandedNode));

        // Update the keyword occurrences map to contain the adjacent tuple set's
        // keywords.
        for (String keyword : adjacent.getKeywords()) {
            jnts.keywordOccurrences.put(keyword, jnts.keywordOccurrences.getOrDefault(keyword, 0) + 1);
        }

        // Update the variables of the network since a new node was added.
        jnts.updateVariables(adjacent, expandedNode, tupleSetGraph);

        return jnts;
    }

    // Creates a deep copy of a network and returns its root node.
    // The new network is expanded with the adjacent tuple set.
    // Keeps a reference to the expanded node in the expandedNode parameter.
    private Node copyNetwork(TupleSet tupleSet, TupleSet adjacent, int currentDepth, int depth, Node node, Node parent,
            Node expandedNode) {
        if (node == null)
            return null;

        Node copyNode = new Node(node.tupleSet, parent);
        for (Node child : node.children) {
            copyNode.children
                    .add(copyNetwork(tupleSet, adjacent, currentDepth + 1, depth, child, copyNode, expandedNode));
        }

        // Attach the adjacent tuple set Ri^K to its appropriate neighbor.
        if ((copyNode.tupleSet.equals(tupleSet) && (currentDepth == depth))) {
            copyNode.children.add(new Node(adjacent, copyNode));
            expandedNode.shallowCopy(copyNode); // Save a reference to return to the caller function.
        }

        return copyNode;
    }

    // Updates the variable of the network after it was expanded.
    private void updateVariables(TupleSet adjacent, Node expandedNode, TupleSetGraph tupleSetGraph) {
        TupleSet tupleSet = expandedNode.tupleSet;
        this.size++;

        // Check if either of the two nodes is a free tuple set.
        if (tupleSet instanceof FreeTupleSet && expandedNode.wasLeaf())
            this.freeTupleSetLeaves--;
        if (adjacent instanceof FreeTupleSet)
            this.freeTupleSetLeaves++;

        // The new node added is a leaf, so the total number of leaves in the network
        // depends on if the expanded node was a leaf before the new node was connected
        // to it.
        // Thus, we only increase the total leaves count if expanded node was not a
        // leaf.
        if (!expandedNode.wasLeaf())
            this.totalLeaves++;

        // Update the pruning condition variable.
        if (expandedNode.hasParent()) {
            TupleSet parentTupleSet = expandedNode.parent.tupleSet;

            if (parentTupleSet.getTable().equals(adjacent.getTable())) {
                if ((tupleSetGraph.getDirectedConnection(parentTupleSet, tupleSet))
                        && (tupleSetGraph.getDirectedConnection(adjacent, tupleSet))) {
                    this.violatesPruningCondition = true;
                }
            }
        }
    }

    // Returns a list of joinable expression pairs that are adjacent in the network.
    public List<JoinablePair> getAdjacentJoinablePairs() {
        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.
        List<JoinablePair> joinablePairs = new ArrayList<JoinablePair>();

        queue.add(this.root);
        while (!queue.isEmpty()) {
            Node parent = queue.remove(); // The parent node.

            // Create a joinable pair for every parent-child combination.
            for (Node child : parent.children) {
                joinablePairs.add(new JoinablePair(parent.getJoinableExpression(), child.getJoinableExpression()));
                queue.add(child);
            }
        }

        return joinablePairs;
    }

    // Rewrite the tree to use the intermediate result given by the JoinablePair
    // object.
    // For rewriting we replace two nodes with an Intermediate Result Node.
    // The new node will be a concatenation of the two old nodes. The old node's
    // Joinable Expressions are now used by the Plan Generator as an intermediate
    // result,
    // and we need to depict that to the Candidate Network also.
    //
    // The Joinable Pair that was previously created by the
    // getAdjacentJoinablePairs()
    // function and now is an Intermediate result helps us find the 2 adjacent nodes
    // that we need to merge in a new Intermediate Result Node.
    public void rewriteJntsUsingIntermediateResult(JoinablePair intermediateResult) {
        if (this.size == 0)
            return; // Can't rewrite a tree containing only a root.
        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.

        // Create the new intermediate result node.
        IntermediateResultNode intermediateNode = new IntermediateResultNode(intermediateResult);

        queue.add(this.root);
        while (!queue.isEmpty()) {
            Node node = queue.remove();

            // Check if the node and its children contain the pairs expressions.
            Node childContainingExpression = node.containsExpressionsOfJoinablePair(intermediateResult);
            if (childContainingExpression == null) {
                // Add children in queue.
                for (Node child : node.children) {
                    queue.add(child);
                }
                continue; // Continue the search
            } else {
                // Merge Node and childContainingExpression to intermediateNode.
                intermediateNode.updateIntermediateNode(node, childContainingExpression, this);
                this.size--;
                break; // Re-writing tree completed.
            }
        }
    }

    // ======================================================
    // Override Functions: equals(), hashCode(), toString()
    // ======================================================

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof JoiningNetworkOfTupleSets))
            return false;
        // Type cast obj
        JoiningNetworkOfTupleSets jnts = (JoiningNetworkOfTupleSets) obj;

        // If the trees have different sizes then return false.
        if (this.size != jnts.size)
            return false;

        // Check the special case where jnts and this have only roots.
        if (this.size == 0)
            return this.getRootsJoinableExpression().equals(jnts.getRootsJoinableExpression());

        // System.out.println("----\n" +
        // "Equals for this:\n" + this +
        // "\nAnd that:\n" + jnts + "\n-----"
        // );

        // If not false is returned then they are the same.
        return this.parallelBfsEquals(jnts);
    }

    // Traverse this and jnts Trees parallel in a BFS way.
    // Return true if they are equal.
    public boolean parallelBfsEquals(JoiningNetworkOfTupleSets jnts) {
        if (this.size != jnts.size)
            return false;
        Queue<Node> thisQueue = new LinkedList<Node>(); // For the breadth first traversal.
        Queue<Node> jntsQueue = new LinkedList<Node>(); // For the breadth first traversal.

        // Check the roots to see if they are they are different
        // If so then return false because they are not the same trees
        if (this.getRootsJoinableExpression().equals(jnts.getRootsJoinableExpression()) == false)
            return false;

        // Add both roots in the tree
        thisQueue.add(this.root);
        jntsQueue.add(jnts.root);

        // Because both trees have the same nodes the two queues
        // will empty at the same time.
        while (!thisQueue.isEmpty() && !jntsQueue.isEmpty()) {
            Node thisNode = thisQueue.remove();
            Node jntsNode = jntsQueue.remove();

            // System.out.println("ThisNode : " +
            // thisNode.getJoinableExpressionsAbbreviation());
            // System.out.println("JntsNode : " +
            // jntsNode.getJoinableExpressionsAbbreviation());

            // Get both node's children
            List<Node> thisNodeChildren = thisNode.children;
            List<Node> jntsNodeChildren = jntsNode.children;

            // Check if their Node tupleSets are equal but in different order.
            // And get the reordered Jnts List to look like thisNodeChildren list.
            List<Node> reorderedJntsNodeChildren = checkTupleSetsEqualityAndReorder(thisNodeChildren, jntsNodeChildren);

            // The Lists are not equal return false.
            if (reorderedJntsNodeChildren == null) {
                return false;
            }

            // Else add both children to the queues.
            for (Node child : thisNodeChildren) {
                thisQueue.add(child);
            }
            for (Node child : reorderedJntsNodeChildren) {
                jntsQueue.add(child);
            }

            // System.out.println();
        }

        // System.out.println("EQUAL");

        // If the check passed and didn't return false then they are equal.
        return true;
    }

    // Check if the TupleSets of the Node Lists are the same but in a
    // different order. If thats the case then reorder one node list
    // and return it. In any other case return null.
    public List<Node> checkTupleSetsEqualityAndReorder(List<Node> thisNodeChildren, List<Node> jntsNodeChildren) {
        // Check if node lists have the same size.
        if (thisNodeChildren.size() != jntsNodeChildren.size()) {
            return null;
        }

        // If the TupleSets are equal reorder the
        // jnts list to look like this list.
        List<Node> reorderedJntsList = new ArrayList<>();

        // Fill the two lists of TupleSets. Their indexes are
        // the same with the Node lists.
        List<TupleSet> thisTupleSets = getTupleSetsOfNodeList(thisNodeChildren);
        List<TupleSet> jntsTupleSets = getTupleSetsOfNodeList(jntsNodeChildren);
        if (thisTupleSets.size() != jntsTupleSets.size()) {
            return null;
        } // It cant be but just to be sure.

        // System.out.print("ThisNodeChildren : [");
        // for (TupleSet tupleSet: thisTupleSets) {
        // System.out.print(tupleSet.toAbbreviation() + ", ");
        // }
        // System.out.print("]\nJntsNodeChildren : [");
        // for (TupleSet tupleSet: jntsTupleSets) {
        // System.out.print(tupleSet.toAbbreviation() + ", ");
        // }
        // System.out.println("]");

        // The sizes of the Lists are the same so we need to see if
        // every tuple Set of one list is inside another.
        for (int pos = 0; pos < thisTupleSets.size(); pos++) {
            TupleSet thisTupleSet = thisTupleSets.get(pos);
            int index = jntsTupleSets.indexOf(thisTupleSet);
            if (index == -1) {
                return null;
            } // thisTupleSet.tupleSet is not in jntsTupleSets.

            // Else add the Node with index to the reorder list.
            reorderedJntsList.add(jntsNodeChildren.get(index));
        }

        // System.out.print("]\nJntsReorderedNodeChildren : [");
        // for (Node node: reorderedJntsList) {
        // System.out.print(node.getJoinableExpressionsAbbreviation() + ", ");
        // }
        // System.out.println("]");

        return reorderedJntsList;
    }

    // Returns the TupleSets from a list of Nodes.
    public List<TupleSet> getTupleSetsOfNodeList(List<Node> nodes) {
        List<TupleSet> tupleSets = new ArrayList<>();
        for (Node node : nodes) {
            tupleSets.add(node.getTupleSet());
        }
        return tupleSets;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = 31 * hash + this.size;
        int treeHashCode = recTreeHashCode(this.root);
        return hash * 31 + treeHashCode;
    }

    // Recursively create the tree's hashCode. Its the sum
    // of all joinable Expressions hashCodes in the tree.
    private int recTreeHashCode(Node node) {
        // Get the parents HashCode.
        int hashCode = node.getJoinableExpression().hashCode();

        // Get the children's HashCode.
        int childrenHashCode = 1;
        for (Node child : node.children) {
            // Sum the children's HashCode.
            childrenHashCode += recTreeHashCode(child);
        }

        // Add the hashCodes.
        return hashCode + childrenHashCode;
    }

    @Override
    public String toString() {
        Queue<Pair<Node, Integer>> queue = new LinkedList<>(); // For the breadth first traversal.
        String str = new String();
        int depth = 0;

        queue.add(new Pair<Node, Integer>(this.root, depth));
        while (!queue.isEmpty()) {
            Pair<Node, Integer> pair = queue.remove();
            Node node = pair.getLeft();
            depth = pair.getRight();

            str += PrintingUtils.addPrefixCharNumTimesToStr(depth, node.getJoinableExpressionsToStr() + ": [", "  ");

            for (Node child : node.children) {
                str += child.getJoinableExpressionsToStr() + ", ";
                queue.add(new Pair<Node, Integer>(child, depth + 1));
            }

            // Remove the last ", "
            if (!node.children.isEmpty()) {
                str = str.substring(0, str.length() - 2);
            }

            str += "]\n";

            // Print tuples of node
            // str += "TUPLES\n";
            // for (SQLTuple tuple: node.getTupleSet().getTuples()){
            // str += tuple + "\n";
            // }

        }

        str += "--Keywords: " + this.keywordOccurrences;
        str += " Size: " + this.size;
        str += " Non-free tuple sets: " + this.freeTupleSetLeaves;
        str += " Total Leaves: " + this.totalLeaves;

        return str;
    }

    @Override
    public String toAbbreviation() {
        Queue<Pair<Node, Integer>> queue = new LinkedList<>(); // For the breadth first traversal.
        String str = new String();
        int depth = 0;

        queue.add(new Pair<Node, Integer>(this.root, depth));
        while (!queue.isEmpty()) {
            Pair<Node, Integer> pair = queue.remove();
            Node node = pair.getLeft();
            depth = pair.getRight();

            str += node.getJoinableExpressionsToStr() + ": [" ;

            for (Node child : node.children) {
                str += child.getJoinableExpressionsToStr() + ", ";
                queue.add(new Pair<Node, Integer>(child, depth));
            }

            // Remove the last ", "
            if (!node.children.isEmpty()) {
                str = str.substring(0, str.length() - 2);
            }

            str += "] ";
        }

        return str;
    }


    @Override
    public boolean containsIntermediateResult(IntermediateResultAssignment intermediateResult) {
        return false;
    }

    @Override
    public boolean containsOrCreatedByIntermediateResult(IntermediateResultAssignment intermediateResult) {
        return false;
    }

    @Override
    public boolean removeIntermediateResultAssignment(IntermediateResultAssignment intermediateResult) {
        return true;
    }

    @Override
    public SQLTable getTable() {
        return null;
    }

    @Override
    public void setTable(SQLTable table) {

    }

    @Override
    public Set<SQLColumn> getColumnsContainingKeywords() {
        return null;
    }

    @Override
    public Set<String> getContainedBaseTables() {
        Set<String> tables = new HashSet<>();
        Queue<Node> queue = new LinkedList<>(); // For the breadth first traversal.
        queue.add(this.root);

        while (!queue.isEmpty()) {
            Node node = queue.remove();

            tables.add(node.getTupleSet().getTable().getName());

            for (Node child : node.children) {
                queue.add(child);
            }
        }

        return tables;
    }

}
