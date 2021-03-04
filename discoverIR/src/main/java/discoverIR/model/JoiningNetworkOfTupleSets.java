package discoverIR.model;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTuple;
import shared.util.Pair;
import shared.util.PrintingUtils;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Queue;
import java.util.LinkedList;

// A joining network of tuple sets J is a tree of tuple sets
// where for each pair of adjacent tuple sets Ri^Q, Rj^Q in J
// there is an an edge between the tables Ri Rj in the schema graph.
public class JoiningNetworkOfTupleSets {

    private static int sizeOfTreeWithRootOnly = 1;

    private Node root; // The root of the network.
    private int size; // The size of the network (number of tuple sets).
    private List<TupleSet> networkTupleSets; // All the tuple sets in the network.

    private int nonFreeTupleSetsCount; // The number of non free tuple sets in the network.
    private int freeTupleSetLeavesCount; // The number of free tuple sets as leaves in the network.
    private int totalLeaves; // The total number of leaves in the network.
    private boolean violatesPruningCondition; // Indicates whether a JNTS violates the pruning condition.

    private Set<String> networkKeywords; // The keywords contained in the network.

    // These values are needed for the scoring formulas and are computed once for each network.
    protected double sizeNormalizationFactor; // The Size Normalization Factor of the network as defined in the paper.
    protected Map<String, Double> keywordIdfs; // The idf values of the keywords contained in the network.
    protected double uscoreB; // The uscore_b (upper bound of the real score_b) value of the network.
    protected double sumidf; // The sumidf value of a network.

    // Statistics
    private static int timesEqualsWasCalled = 0;

    public static void printStats() {
        System.out.println("Global JoiningNetworkOfTupleSets stats:");
        System.out.println("Times equals called: " + timesEqualsWasCalled);
    }

    // Copy constructor (only copies the variables).
    public JoiningNetworkOfTupleSets(JoiningNetworkOfTupleSets src) {
        this.root = null;
        this.size = src.getSize();
        this.networkTupleSets = new ArrayList<TupleSet>(src.networkTupleSets);

        this.nonFreeTupleSetsCount = src.getNonFreeTupleSetsCount();
        this.freeTupleSetLeavesCount = src.getFreeTupleSetLeaves();
        this.totalLeaves = src.getTotalLeaves();
        this.violatesPruningCondition = src.getViolatesPruningCondition();

        this.networkKeywords = new HashSet<String>(src.getNetworkKeywords());

        this.sizeNormalizationFactor = src.getSizeNormalizationFactor();
        this.keywordIdfs = new HashMap<String, Double>(src.getKeywordIdfs());
        this.uscoreB = src.getUScoreB();
        this.sumidf = src.getSumidf();
    }

    // Creates a new JNTS with the given tuple set as a root node.
    public JoiningNetworkOfTupleSets(TupleSet tupleSet) {
        this.root = new Node(tupleSet);
        this.size = 1;
        this.networkTupleSets = new ArrayList<TupleSet>();
        this.networkTupleSets.add(tupleSet);

        this.nonFreeTupleSetsCount = 1; // The network is always initialized with a non-free tuple set.
        this.freeTupleSetLeavesCount = 0;
        this.totalLeaves = 1; // The root is a leaf.
        this.violatesPruningCondition = false;

        // Add the keywords of the tuple set.
        this.networkKeywords = new HashSet<String>(tupleSet.getKeywords());

        this.sizeNormalizationFactor = 0.0;
        this.keywordIdfs = new HashMap<String, Double>();
        this.uscoreB = 0.0;
        this.sumidf = 0.0;
    }

    // Getters and Setters.
    public int getSize() {
        return this.size;
    }

    public int getNonFreeTupleSetsCount() {
        return this.nonFreeTupleSetsCount;
    }

    public int getFreeTupleSetLeaves() {
        return this.freeTupleSetLeavesCount;
    }

    public int getTotalLeaves() {
        return this.totalLeaves;
    }

    public Set<String> getNetworkKeywords() {
        return this.networkKeywords;
    }

    public void setNetworkKeywords(Set<String> networkKeywords) {
        this.networkKeywords = networkKeywords;
    }

    public void setNetworkKeywords(List<String> networkKeywords) {
        this.networkKeywords.addAll(networkKeywords);
    }

    public Node getRoot() {
        return root;
    }

    public void setRoot(Node root) {
        this.root = root;
    }

    public boolean getViolatesPruningCondition() {
        return this.violatesPruningCondition;
    }

    public double getSizeNormalizationFactor() {
        return this.sizeNormalizationFactor;
    }

    public void setSizeNormalizationFactor(Double sizeNormalizationFactor) {
        this.sizeNormalizationFactor = sizeNormalizationFactor;
    }

    public Map<String, Double> getKeywordIdfs() {
        return this.keywordIdfs;
    }

    public double getUScoreB() {
        return this.uscoreB;
    }

    public double getSumidf() {
        return this.sumidf;
    }

    // Returns a Set of the unique non free tuple sets of the network.
    // TODO can we make it more efficient: TupleSets equality also checks their tuples !
    public Set<TupleSet> getUniqueNonFreeTupleSets() {
        Set<TupleSet> nonFreeTupleSets = new HashSet<TupleSet>();
        for (TupleSet tupleSet : this.networkTupleSets) {
            if (!(tupleSet instanceof FreeTupleSet)) {
                nonFreeTupleSets.add(tupleSet);
            }
        }

        return nonFreeTupleSets;
    }

    // Returns a list of the non free tuple sets of the network. (contains duplicates)
    public List<TupleSet> getNonFreeTupleSets() {
        List<TupleSet> nonFreeTupleSets = new ArrayList<TupleSet>();
        for (TupleSet tupleSet : this.networkTupleSets) {
            if (!(tupleSet instanceof FreeTupleSet)) {
                nonFreeTupleSets.add(tupleSet);
            }
        }

        return nonFreeTupleSets;
    }

    // Returns a list of the free tuple sets of the network.
    public List<TupleSet> getFreeTupleSets() {
        List<TupleSet> freeTupleSets = new ArrayList<TupleSet>();
        for (TupleSet tupleSet : this.networkTupleSets) {
            if (tupleSet instanceof FreeTupleSet) {
                freeTupleSets.add(tupleSet);
            }
        }

        return freeTupleSets;
    }

    // Return all the columns Containing Keywords from this tree's TupleSets.    
    public Set<SQLColumn> getColumnsContainingKeywords() {
        Queue<Node> queue = new LinkedList<>(); // For the BFS traversal.
        Set<SQLColumn> columnsSet = new HashSet<>();

        // Add roots TupleSet to the result List.
        columnsSet.addAll(this.root.tupleSet.getColumnsContainingKeywords());

        // Traverse the tree.
        queue.add(this.root);
        while(!queue.isEmpty()) {
            Node parent = queue.remove();

            // Get the children and add their ColumnsSet to the result List.
            for(Node child: parent.children) {
                columnsSet.addAll(child.tupleSet.getColumnsContainingKeywords());
                // Add the child to the queue.
                queue.add(child);
            }

        }

        return columnsSet;
    }

    // Creates a deep copy of a network and returns its root node.
    // The new network is expanded with the adjacent tuple set.
    // Keeps a reference to the expanded node in the expandedNode parameter.
    private Node copyNetwork(TupleSet tupleSet, TupleSet adjacent, int currentDepth, int depth,
            Node node, Node parent, Node expandedNode) {
        if (node == null) return null;

        Node copyNode = new Node(node.tupleSet, parent);
        for (Node child : node.children) {
            copyNode.children.add(copyNetwork(tupleSet, adjacent, currentDepth+1, depth, child, copyNode, expandedNode));
        }

        // Attach the adjacent tuple set Ri^K to its appropriate neighbor.
        if ((copyNode.tupleSet.equals(tupleSet) && (currentDepth == depth))) {
            copyNode.children.add(new Node(adjacent, copyNode));
            expandedNode.shallowCopy(copyNode); // Save a reference to return to the caller function.
        }

        return copyNode;
    }

    // Returns true if a candidate network J violates the pruning condition:
    // A candidate network J does not contain a subtree of the form R - S - R,
    // where R and S are relations and the schema graph has an edge R -> S.
    public boolean violatesPruningCondition() {
        return this.violatesPruningCondition;
    }

    // Returns true if a candidate network J satisfies the acceptance conditions:
    // 1. The number of non-free tuple sets does not exceed the number of keywords in the query.
    // 2. J does not contain any free tuple sets as leaves.
    // In addition, if the user requested AND semantics to be used the network must contain all keywords.
    public boolean satisfiesAcceptanceConditions(List<String> keywords, boolean andSemantics) {
        boolean conditionOne = (this.nonFreeTupleSetsCount <= keywords.size());
        boolean conditionTwo = (this.freeTupleSetLeavesCount == 0);

        if (andSemantics) {
            boolean containsAllKeywords = (keywords.size() == this.networkKeywords.size());
            return (containsAllKeywords && conditionOne && conditionTwo);
        }
        else {
            return (conditionOne && conditionTwo);
        }
    }

    // A network cannot have more than k-m leaves, where k is the number of all keywords in the query
    // and m is the number of keywords already in the tuple sets of the network.
    // Returns true if the network satisfies the above condition.
    public boolean satisfiesLeavesCondition(int allKeywords) {
        int networkKeywordsCount = this.networkKeywords.size();
        return (allKeywords != networkKeywordsCount) ? (this.totalLeaves <= allKeywords - networkKeywordsCount) : true;
    }

    // Computes the constant values of the network such as the Size Normalization Factor,
    // the map with the keyword idf values, the uscoreB value, and the sumidf value.
    // Receives a list with all the keywords of the query as input.
    public void computeConstantValues(List<String> queryKeywords) {
        this.sizeNormalizationFactor = this.computeSizeNormalizationFactor(queryKeywords.size());
        this.computeKeywordIdfs();
        this.uscoreB = this.computeUScoreB(queryKeywords);
        this.sumidf = this.computeSumidf();
    }

    // Computes and returns the Size Normalization Factor of the network.
    private double computeSizeNormalizationFactor(int queryKeywordsNum) {
        // Two constant values defined in the paper.
        double s1 = 0.15;
        double s2 = 1.0 / ((double) queryKeywordsNum + 1.0);

        // Compute the two terms of the product.
        double term1 = 1.0 + s1 - (s1 * this.getSize());
        double term2 = 1.0 + s2 - (s2 * this.getNonFreeTupleSetsCount());

        return term1 * term2;
    }

    // Computes and stores the idf value of every keyword contained in the network.
    private void computeKeywordIdfs() {
        for (String keyword : this.networkKeywords) {
            Double product = 1.0;

            // Get the selectivity (p_w(Rj)) of the keyword for every tuple set that contains it.
            for (TupleSet tupleSet : this.getNonFreeTupleSets()) {
                if (tupleSet.containsKeyword(keyword)) {
                    product *= (1.0 - tupleSet.getKeywordSelectivity(keyword));
                }
            }

            this.keywordIdfs.put(keyword, 1.0 / (1.0 - product));
        }
    }

    // Returns the maximum idf value from the keywordIdfs map.
    private Double getMaximumIdf() {
        Double max = -1.0;

        for (Double idf : this.keywordIdfs.values()) {
            if (idf > max) {
                max = idf;
            }
        }

        return max;
    }

    // Computes and returns the uscore_b (an upper bound of the real score_b) value of a network.
    private double computeUScoreB(List<String> queryKeywords) {
        // A constant value defined in the paper.
        // Switches the semantics from OR to AND when it increases to infinity. (A value of 2.0 is enough.)
        double p = 1.0;

        // Compute the value of the summation in the formula.
        double sum = 0.0;
        for (String keyword : queryKeywords) {
            // Assume that T.i=1 for every keyword that is contained in the network, and T.i=0 otherwise.
            // Thus, only keywords not contained in the network contribute to the sum value.
            if (!this.networkKeywords.contains(keyword)) {
                sum += Math.pow(1.0, p);
            }
        }

        return (1.0 - Math.pow((sum / queryKeywords.size()), (1.0 / p)));
    }

    // Computes and returns the sumidf value of a network.
    // The sumidf is the sum of the idf values of every keyword contained in hte network.
    private double computeSumidf() {
        Double sum = 0.0;

        // Add the idf of every keyword contained in the network.
        for (String keyword : this.networkKeywords) {
            sum += this.keywordIdfs.get(keyword);
        }

        return sum;
    }

    // Computes and returns the maximum possible score (MPS) of the network by adding the score
    // of the top tuple from every non-free tuple set in the network.
    public Double computeMaximumPossibleScore() {
        Double mps = 0.0;

        for (TupleSet tupleSet : this.getNonFreeTupleSets()) {
            mps += tupleSet.getTopTuple().getScore();
        }

        return (mps / this.nonFreeTupleSetsCount);
    }

    // Computes and returns the maximum possible future score (MPFS) of the network when a tuple set is specified.
    // It is computed by adding the score of the top tuple from every non-free tuple set in the network
    // except for the given tuple set, for which we add the score of the first not retrived tuple
    // whose position is specified by the given prefix.
    public Double computeMaximumPossibleFutureScore(TupleSet chosenTupleSet, int prefix) {
        Double mpfs = 0.0;

        for (TupleSet tupleSet : this.getNonFreeTupleSets()) {
            if (tupleSet.equals(chosenTupleSet)) {
                mpfs += tupleSet.getTupleByIndex(prefix).getScore();
            }
            else {
                mpfs += tupleSet.getTopTuple().getScore();
            }
        }

        return (mpfs / this.size);
        // return mpfs;
    }

    // Computes the global maximum possible future score (MPFS) of the network
    // by looping through the tuple sets and selecting one every time as the chosen one.
    // Receives the non-free tuple sets of the network and their corresponding prefixes.
    // Returns the MPFS value paired with the tuple set that resulted in that value.
    public Pair<Double, TupleSet> computeGlobalMaximumPossibleFutureScore(List<TupleSet> tupleSets, int[] prefixes) {
        Double globalMpfs = 0.0;
        TupleSet chosenTupleSet = null;

        for (TupleSet tupleSet : tupleSets) {
            // Skip the tuple sets whose tuples have all been retrieved.
            int prefix = prefixes[tupleSets.indexOf(tupleSet)];
            if (prefix == tupleSet.getSize()) continue;

            // Compute the score by setting the current tuple set as the chosen one.
            Double mpfs = this.computeMaximumPossibleFutureScore(tupleSet, prefix);

            if (mpfs > globalMpfs) {
                globalMpfs = mpfs;
                chosenTupleSet = tupleSet;
            }
        }

        // return (globalMpfs / this.nonFreeTupleSetsCount);
        return new Pair<>(globalMpfs, chosenTupleSet);
    }

    // Computes the global maximum possible future score (MPFS) of the network
    // by looping through the tuple sets and selecting one every time as the chosen one.
    // Receives the non-free tuple sets of the network and their corresponding prefixes.
    // Returns the MPFS value.
    public Double computeGlobalMaximumPossibleFutureScoreOnly(List<TupleSet> tupleSets, int[] prefixes) {
        Double globalMpfs = 0.0;

        for (TupleSet tupleSet : tupleSets) {
            // Skip the tuple sets whose tuples have all been retrieved.
            int prefix = prefixes[tupleSets.indexOf(tupleSet)];
            if (prefix == tupleSet.getSize()) continue;

            // Compute the score by setting the current tuple set as the chosen one.
            Double mpfs = this.computeMaximumPossibleFutureScore(tupleSet, prefix);

            if (mpfs > globalMpfs) {
                globalMpfs = mpfs;
            }
        }
        
        return globalMpfs;
    }

    // Performs a breadth first traversal of the tree and returns a list
    // of the adjacent tuple sets of every node (of the tree) in the tuple set graph.
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
                adjacent.add(adjacentOfCurrentNode);
            }

            // Add the children of the node to the queue.
            for (Node child : node.children) {
                queue.add(child);
            }
        }

        return adjacent;
    }

    // Returns true if a candidate network J and a given tuple set Ri^K satisfies the expansion rule.
    // Performs a breadth first traversal to apply the rule for every tuple set Rj^M.
    // The given tuple set is the Ri^K to be checked for insertion in the network.
    public boolean checkExpansionRule(TupleSet tupleSet) {
        // If K = {} (thus, Ri^K is a free tuple set) it is accepted.
        if (tupleSet instanceof FreeTupleSet)  return true;

        if (this.networkTupleSets.contains(tupleSet)) return false;

        return true;
    }

    // Creates a new JNTS by attaching the 'adjacent' tuple set to the current network.
    // The depth variable is needed to differentiate between multiple occurrences
    // of the same tuple set in the network, and treat them independently.
    // It creates a deep copy of the current network while expanding it.
    public JoiningNetworkOfTupleSets expand(TupleSet tupleSet, TupleSet adjacent, int depth, TupleSetGraph tupleSetGraph) {
        Node expandedNode = new Node(); // The node that the adjacent tupleSet will be attached to.

        // Create a deep copy of the current network and expand it with the adjacent tuple set.
        JoiningNetworkOfTupleSets network = new JoiningNetworkOfTupleSets(this);
        network.setRoot(copyNetwork(tupleSet, adjacent, 0, depth, this.root, null, expandedNode));

        // Update the networkKeywords set to contain the adjacent tuple set's keywords.
        network.networkKeywords.addAll(adjacent.getKeywords());

        // Update the variables of the network since a new node was added.
        network.updateVariables(adjacent, expandedNode, tupleSetGraph);

        return network;
    }

    // Updates the variable of the network after it was expanded.
    private void updateVariables(TupleSet adjacent, Node expandedNode, TupleSetGraph tupleSetGraph) {
        TupleSet tupleSet = expandedNode.tupleSet;

        this.size++;
        this.networkTupleSets.add(adjacent);

        // Check if the new node is a non free tuple set.
        if (!(adjacent instanceof FreeTupleSet)) this.nonFreeTupleSetsCount++;

        // Check if either of the two nodes is a free tuple set.
        if (tupleSet instanceof FreeTupleSet && expandedNode.wasLeaf()) this.freeTupleSetLeavesCount--;
        if (adjacent instanceof FreeTupleSet) this.freeTupleSetLeavesCount++;

        // The new node added is a leaf, so the total number of leaves in the network
        // depends on if the expanded node was a leaf before the new node was connected to it.
        // Thus, we only increase the total leaves count if expanded node was not a leaf.
        if (!expandedNode.wasLeaf()) this.totalLeaves++;

        // Update the pruning condition variable.
        if (expandedNode.hasParent()) {
            TupleSet parentTupleSet = expandedNode.parent.tupleSet;

            if (parentTupleSet.getTable().equals(adjacent.getTable())) {
                if ((tupleSetGraph.getDirectedConnection(parentTupleSet, tupleSet)) &&
                    (tupleSetGraph.getDirectedConnection(adjacent, tupleSet))) {
                        this.violatesPruningCondition = true;
                }
            }
        }
    }

    // Returns the TupleSets from a list of Nodes.
    public List<TupleSet> getTupleSetsOfNodeList(List<Node> nodes) {
        List<TupleSet> tupleSets = new ArrayList<>();
        for (Node node: nodes) {
            tupleSets.add(node.getTupleSet());
        }
        return tupleSets;
    }

    // Check if the TupleSets of the Node Lists are the same but in a 
    // different order. If thats the case then reorder one node list
    // and return it. In any other case return null.
    public List<Node> checkTupleSetsEqualityAndReorder(List<Node> thisNodeChildren, List<Node> jntsNodeChildren) {
        // Check if node lists have the same size.
        if (thisNodeChildren.size() != jntsNodeChildren.size()) {return null;}

        // If the TupleSets are equal reorder the 
        // jnts list to look like this list.
        List<Node> reorderedJntsList = new ArrayList<>();

        // Fill the two lists of TupleSets. Their indexes are
        // the same with the Node lists.
        List<TupleSet> thisTupleSets = getTupleSetsOfNodeList(thisNodeChildren);
        List<TupleSet> jntsTupleSets = getTupleSetsOfNodeList(jntsNodeChildren);        
        if (thisTupleSets.size() != jntsTupleSets.size()) {return null;} // It cant be but just to be sure.

        // System.out.print("ThisNodeChildren : [");
        // for (TupleSet tupleSet: thisTupleSets) {
        //     System.out.print(tupleSet.toAbbreviation() + ", ");
        // }
        // System.out.print("]\nJntsNodeChildren : [");
        // for (TupleSet tupleSet: jntsTupleSets) {
        //     System.out.print(tupleSet.toAbbreviation() + ", ");
        // }
        // System.out.println("]");
        
        // The sizes of the Lists are the same so we need to see if         
        // every tuple Set of one list is inside another.
        for (int pos = 0; pos < thisTupleSets.size(); pos++) {
            TupleSet thisTupleSet = thisTupleSets.get(pos);
            int index = jntsTupleSets.indexOf(thisTupleSet);            
            if (index == -1) {return null;} // thisTupleSet.tupleSet is not in jntsTupleSets.

            // Else add the Node with index to the reorder list.
            reorderedJntsList.add(jntsNodeChildren.get(index));
        }

        // System.out.print("]\nJntsReorderedNodeChildren : [");
        // for (Node node: reorderedJntsList) {
        //     System.out.print(node.getJoinableExpressionsAbbreviation() + ", ");
        // }
        // System.out.println("]");

        return reorderedJntsList;
    }


    // Traverse this and jnts Trees parallel in a BFS way.
    // Return true if they are equal.
    public boolean parallelBfsEquals(JoiningNetworkOfTupleSets jnts) {
        if (this.size != jnts.size) return false;        
        Queue<Node> thisQueue = new LinkedList<Node>(); // For the breadth first traversal.
        Queue<Node> jntsQueue = new LinkedList<Node>(); // For the breadth first traversal.
        
        // Add both roots in the tree
        thisQueue.add(this.root);
        jntsQueue.add(jnts.root);

        // Because both trees have the same nodes the two queues
        // will empty at the same time. 
        while (!thisQueue.isEmpty() && !jntsQueue.isEmpty() ) {
            Node thisNode = thisQueue.remove();
            Node jntsNode = jntsQueue.remove();

            // System.out.println("ThisNode : "  + thisNode.getJoinableExpressionsAbbreviation());
            // System.out.println("JntsNode : "  + jntsNode.getJoinableExpressionsAbbreviation());

            // Get both node's children
            List<Node> thisNodeChildren = thisNode.children;
            List<Node> jntsNodeChildren = jntsNode.children;
            
            // Check if their Node tupleSets are equal but in different order.
            // And get the reordered Jnts List to look like thisNodeChildren list.
            List<Node> reorderedJntsNodeChildren = checkTupleSetsEqualityAndReorder(
                thisNodeChildren, jntsNodeChildren
            );

            // The Lists are not equal return false.
            if (reorderedJntsNodeChildren == null) {return false;}

            // Else add both children to the queues.
            for (Node child: thisNodeChildren) {
                thisQueue.add(child);
            }
            for (Node child: jntsNodeChildren) {
                jntsQueue.add(child);
            }
            
            // System.out.println();
        }

        // System.out.println("EQUAL");

        // If the check passed and didn't return false then they are equal.
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof JoiningNetworkOfTupleSets)) return false;
        JoiningNetworkOfTupleSets.timesEqualsWasCalled++;
        // Type cast obj
        JoiningNetworkOfTupleSets jnts = (JoiningNetworkOfTupleSets) obj;

        // If the trees have different sizes then return false.
        if (this.size != jnts.size) return false;        

        // Check the special case where jnts and this have only roots.
        if (this.size == sizeOfTreeWithRootOnly)
            return this.getRoot().getTupleSet().equals(jnts.getRoot().getTupleSet());
        
        // System.out.println("----\nEquals for this:\n" + this +
        //     "\nJnts:\n" + jnts + "-----"
        // );
        
        // If no false is returned then they are the same.
        return this.parallelBfsEquals(jnts);
    }

    // Recursively create the tree's hashCode. Its the sum
    // of all joinable Expressions hashCodes in the tree.
    private int recTreeHashCode(Node node) {
        // Get the parents HashCode.
        int hashCode = node.getTupleSet().hashCode();

        // Get the children's HashCode.
        int childrenHashCode = 1;
        for (Node child: node.children) {
            // Sum the children's HashCode.
            childrenHashCode += recTreeHashCode(child);
        }

        // Add the hashCodes.
        return hashCode + childrenHashCode;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = 31 * hash + this.size;
        int treeHashCode = recTreeHashCode(this.root);
        return hash*31 + treeHashCode;
    }

    public String toAbbreviation() {
        Queue<Node> queue = new LinkedList<>(); // For the BFS traversal.
        String nodesAbbreviations = new String("{");

        // Add roots TupleSet to the result List.
        nodesAbbreviations += this.root.getJoinableExpressionsAbbreviation() + ", ";

        // Traverse the tree.
        queue.add(this.root);
        while(!queue.isEmpty()) {
            Node parent = queue.remove();

            // Get the children and add their ColumnsSet to the result List.
            for(Node child: parent.children) {
                nodesAbbreviations += child.getJoinableExpressionsAbbreviation() + ", ";
                // Add the child to the queue.
                queue.add(child);
            }
        }
        // Remove the last ", "
        nodesAbbreviations = nodesAbbreviations.substring(0, nodesAbbreviations.length()-2) + "}";

        return nodesAbbreviations;
    }

    public String debugPrint() {
        Queue<Pair<Node, Integer>> queue = new LinkedList<>(); // For the breadth first traversal.
        String str = new String();
        int depth = 0;

        queue.add(new Pair<Node, Integer>(this.root, depth));
        while (!queue.isEmpty()) {
            Pair<Node, Integer> pair = queue.remove();
            Node node = pair.getLeft();
            depth = pair.getRight();

            str += PrintingUtils.addPrefixCharNumTimesToStr(
                depth,
                node.getJoinableExpressionsAbbreviation() + ": [",
                "  "
            );

            for (Node child : node.children) {
                str += child.getJoinableExpressionsAbbreviation() + ", ";
                queue.add(new Pair<Node, Integer>(child, depth + 1));
            }

            // Remove the  last ", "
            if (!node.children.isEmpty()) {
                str = str.substring(0, str.length() - 2);
            }

            str += "]\n";

            //  Print tuples of node
            str += "TUPLES\n";
            for (SQLTuple tuple: node.getTupleSet().getTuples()){
                str += tuple + "\n";
            }

        }

        str += "--Keywords: " + this.networkKeywords;
        str += " Size: " + this.size;
        str += " Non-free tuple sets: " + this.nonFreeTupleSetsCount;
        str += " Free Leaves: " + this.freeTupleSetLeavesCount;
        str += " Total Leaves: " + this.totalLeaves;

        return str;
    }

    @Override
    public String toString() {
        Queue<Node> queue = new LinkedList<Node>(); // For the breadth first traversal.
        String str = new String();

        queue.add(this.root);
        while (!queue.isEmpty()) {
            Node node = queue.remove();
            str += node.getJoinableExpressionsAbbreviation() + ": [";

            for (Node child : node.children) {
                str += child.getJoinableExpressionsAbbreviation() + ", ";
                queue.add(child);
            }

            // Remove the  last ", "
            if (!node.children.isEmpty()) {
                str = str.substring(0, str.length() - 2);
            }

            str += "] ";
        }

        str += "\nKeywords: " + this.networkKeywords;
        str += " Size: " + this.size;
        str += " Non-free tuple sets: " + this.nonFreeTupleSetsCount;
        str += " Free Leaves: " + this.freeTupleSetLeavesCount;
        str += " Total Leaves: " + this.totalLeaves;

        return str;
    }

}
