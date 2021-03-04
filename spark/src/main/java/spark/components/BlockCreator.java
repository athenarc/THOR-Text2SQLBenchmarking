package spark.components;

import spark.SparkApplication;
import spark.model.Block;
import spark.model.FreeTupleSet;
import spark.model.JoiningNetworkOfTupleSets;
import spark.model.Node;
import spark.model.TupleSet;
import spark.model.Stratum;
import spark.model.Signature;
import spark.model.ScoreType;
import shared.util.Pair;


import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

// The Block creator class is responsible for creating blocks for a Candidate Network.
// Each block created by this object is unique. Also the block is assigned with its
// creator. This happens to make the creator easy to access when the block needs to create
// it's adjacent blocks.
public class BlockCreator {

    // This class is used to represent the created blocks as vectors of integers, where the value of a dimension
    // corresponds to the stratum selected fro the block from the tuple set of that dimension.
    public static class IndexArray {        
        
        int[] array;

        public IndexArray(int[] array) {
            this.array = array;
        }
        
        // Performs a deep clone on the object.
        public IndexArray clone() {
            int[] clone = this.array.clone();
            return new IndexArray(clone); 
        }

        // Returns the length of the array.
        public int length() { return this.array.length; }

        // Returns the value in the position specified by the index.
        public int get(int index) { return this.array[index]; }

        // Sets the value in the position specified by the index.
        public void set(int index, int value) {this.array[index] = value; }

        // Increments (by one) the value in the position specified by the index.
        public void increment(int index) { this.array[index]++; }

        @Override
        public int hashCode() {
            int hash = 7;            
            for (int i: this.array)
                hash = hash * i;            
            return 31*hash;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;
            if (!(obj instanceof IndexArray)) return false;            
            IndexArray i = (IndexArray) obj;

            // Check sizes.
            if (this.array.length != i.array.length) return false;

            // Check if the values in every position are equal.
            for (int pos = 0; pos < i.array.length; pos++) {
                if (this.array[pos] != i.array[pos]) return false;
            }

            return true;
        }        

    }

    private Map<String, List<Stratum>> strataPerTupleSet; // Maps a tuple set to its list of strata.
    private JoiningNetworkOfTupleSets network; // The candidate network.
    private Set<IndexArray> blockCache; // The blocks created so far.

    // Splits the non-free tuple sets of the network into strata, too.
    public BlockCreator(JoiningNetworkOfTupleSets network) {
        Set<TupleSet> tupleSets = network.getUniqueNonFreeTupleSets();
        this.network = network;
        this.strataPerTupleSet = new HashMap<>();
        this.blockCache = new HashSet<>();
        
        // Debug Prints
        if (SparkApplication.DEBUG_PRINTS) 
            System.out.println("Creating the Strata for network: " + network.toAbbreviation());


        // Create a map entry for each unique tuple set and create their strata.
        for (TupleSet tupleSet : tupleSets) {
            // Debug Prints
            if (SparkApplication.DEBUG_PRINTS) 
                System.out.println("\tTuple Set " + tupleSet.toAbbreviation() + ":");            
                
            this.strataPerTupleSet.put(tupleSet.getTable().getName(), tupleSet.createStrata(network));
        }
    }

    // Returns a list of blocks adjacent to the block passed as argument.
    // A block is considered adjacent to another block if they contain the same strata,
    // except for one (which is in turn adjacent to one of the other block's strata).
    // Two strata are considered adjacent if they reside in consecutive positions of the same tuple set's list of strata.
    // 
    // So, since blocks can be thought of as vectors of integer indexes, this function loops through that vector,
    // and for every position it creates a copy of the initial vector and increments that position's index by one.
    public List<Block> createAdjacentBlocks(Block block) {
        if (SparkApplication.DEBUG_PRINTS) {
            System.out.println("Network: " + block.toAbbreviation());
            System.out.println("Creating adjacent blocks of block: \n" + block + "\n");
        }

        List<Block> adjacentBlocks = new ArrayList<>();
                
        // Get the indexes of the strata that were used to create the block passed as argument.
        IndexArray strataIndexesOfBlock = block.getStrataIndexesUsed();

        // Get the pairs that map a node in the block tree with the index of the stratum used for that node.
        List<Pair<Integer, Node>> stratumUsedPerNode = block.getStratumUsedPerNode();
                
        // Increment each position of the int[] by 1 and check if the block has 
        // been created before from the block cache.
        if (SparkApplication.DEBUG_PRINTS)
            System.out.println("Loop adjacent (" + strataIndexesOfBlock.length() +  "):");

        for (int pos = 0; pos < strataIndexesOfBlock.length(); pos++) {

            if (SparkApplication.DEBUG_PRINTS)
                System.out.println(
                    "Pos: " + pos + " | index: " + strataIndexesOfBlock.get(pos) + " | strata list size: " +
                    this.strataPerTupleSet.get((stratumUsedPerNode.get(pos).getRight().getTupleSet().getTable().getName())).size() +
                    " | tupleSet: " + stratumUsedPerNode.get(pos).getRight().getTupleSet().toAbbreviation()
                );

            // Check if the tuple set in the current position has any remanining strata.
            if (this.hasMoreStrata(pos, strataIndexesOfBlock.get(pos), stratumUsedPerNode) == false) {

                if (SparkApplication.DEBUG_PRINTS)
                    System.out.println("Can't create block: no more strata for tuple set\n");

                continue;
            }

            // Copy the initial array of indexes an increment the index of the stratum in the current position by one.
            IndexArray strataIndexesForAdjacent = strataIndexesOfBlock.clone();
            strataIndexesForAdjacent.increment(pos);
           
            // Check if the block has been already created, to avoid duplicates.
            if (this.blockCache.contains(strataIndexesForAdjacent)) {

                if (SparkApplication.DEBUG_PRINTS)
                    System.out.println("Block already created!\n");

                continue;
            }

            // Create the adjacent block by duplicating the current one, and changing the stratum
            // of the tuple set in position pos by getting the next one.
            Block adjacent = duplicateBlock(block);
            this.changeStratumOfBlock(pos, strataIndexesForAdjacent.get(pos), adjacent);    

            // Calculate the adjacent block's signature with the new stratum.
            adjacent.computeSignature();

            // Calculate the adjacent block's uscore value.
            adjacent.setStatus(ScoreType.USCORE);
            adjacent.setScore(Double.valueOf(adjacent.computeUScore()));

            if (SparkApplication.DEBUG_PRINTS)
                System.out.println("Adjacent block uscore = " + adjacent.getScore() + "\n");

            // Keep the adjacent block in the cache, so we won't create it again.
            blockCache.add(strataIndexesForAdjacent);

            // Add the adjacent block to the list of adjacent blocks to be returned.
            adjacentBlocks.add(adjacent);

            if (SparkApplication.DEBUG_PRINTS)
                System.out.println("Adjacent block created\n" + adjacent + "\n");            
        }

        if (SparkApplication.DEBUG_PRINTS)
            System.out.println("\n----------------------------------\n");

        return adjacentBlocks;
    }

    // Creates the first block of the candidate network, by combining the first
    // stratum of every non-free tuple set in the network.
    public Block createFirstBlock() {
        Block block = new Block(this.network);
        block.setRoot(extractFirstBlockFromNetwork(block, this.network.getRoot(), null)); // Copies the network nodes.
        block.computeSignature();
        blockCache.add(block.getStrataIndexesUsed()); // Cache the block.
        block.setBlockCreator(this); // Save the block creator to generate the adjacent blocks.

        return block;
    }

    // Returns true if the tuple set in position pos has one or more strata after than index argument.
    private boolean hasMoreStrata(int pos, int index, List<Pair<Integer, Node>> stratumUsedPerNode) {
        String tupleSetName = stratumUsedPerNode.get(pos).getRight().getTupleSet().getTable().getName();
        return (index + 1 < this.strataPerTupleSet.get(tupleSetName).size());
    }

    // Changes the stratum of parameter block to stratum with index newStratumIndex of a specific
    // position in the Block (Tree). The indexes point are on a List<Stratum> in this.stratumPerTupleSet Map.
    private void changeStratumOfBlock(int pos, int newStratumIndex , Block block) {
        // Get the Blocks Triplets, indicating which and where strata are located in the Block.
        List<Pair<Integer, Node>> stratumUsedPerNode = block.getStratumUsedPerNode();

        // Change the Node's tupleSet to the newStratum.
        Node nodeToChange = stratumUsedPerNode.get(pos).getRight();
        String tupleSetName = nodeToChange.getTupleSet().getTable().getName();
        Stratum newStratum = this.strataPerTupleSet.get(tupleSetName).get(newStratumIndex);
        nodeToChange.setTupleSet(newStratum);

        // Change the Integer indexing the newStratum in the List.
        stratumUsedPerNode.get(pos).setLeft(newStratumIndex);
    }

    // Duplicates a Block to a new Block.
    private Block duplicateBlock(Block block) {
        Block duplicate = new Block(block); // Initialize the adjacent block 
        duplicate.setRoot(this.simpleBlockCopy(duplicate, block.getRoot(), null)); // Copy the nodes from block.

        // Add this BlockCreator to the adjacentBlock.
        duplicate.setBlockCreator(this);
        
        return duplicate;
    }    

    // Extracts the first block of a network by copying the nodes.
    // However, the nodes of the block contain strata objects instead of tuple sets.
    // Since this function creates the first block only, it uses the first stratum
    // of every tuple set (index 0) of the initial network.
    private Node extractFirstBlockFromNetwork(Block block, Node node, Node parent) {
        if (node == null) return null;

        Node copyNode = new Node(parent); // Copy the original node.

        // If the node contains a non-free tuple set, then get its first stratum to use for the block's node.
        TupleSet nodeTupleSet = node.getTupleSet();
        if (!(node.getTupleSet() instanceof FreeTupleSet)) {
            nodeTupleSet = this.strataPerTupleSet.get(nodeTupleSet.getTable().getName()).get(0); // The first stratum.

            // Store the stratum index used for this node.
            block.addStratumNodePair(0, copyNode);
        }

        // Set the tuple set (free or stratum) of the block's node.
        copyNode.setTupleSet(nodeTupleSet);

        // Recursively iterate through the node's children.
        for (Node child : node.getChildren()) {            
            copyNode.addChild(extractFirstBlockFromNetwork(block, child, copyNode));
        }

        return copyNode;
    }

    // Copies the Nodes of a Block in a recursively strategy and returns the new Block's root.
    private Node simpleBlockCopy(Block copyBlock, Node node, Node parent) {
        if (node == null) return null;

        // Crete a copy of the Original Tree's Node.
        Node copyNode = new Node(node.getTupleSet(), parent);

        // If the node contains a Stratum as its TupleSet then store the stratum index 
        // used for this copyNode, the copyNode and the node's TupleSet.
        if (node.getTupleSet() instanceof Stratum) {
            Stratum stratum = (Stratum) node.getTupleSet();            
            copyBlock.addStratumNodePair(stratum.getIndex(), copyNode); 
        }

        // Create a copy of this nodes children and add them to the copyNode.
        for (Node child : node.getChildren()) {            
            copyNode.addChild(simpleBlockCopy(copyBlock, child, copyNode));
        }

        return copyNode;
    }

    // Returns the signature of every stratum of the block argument in a list (only from non-free tuple sets).
    public List<Signature> getStrataSignatures(Block block) {
        List<Signature> signatures = new ArrayList<>();

        // Loop the pairs that connect the nodes of the block and the index of the stratum used for the node
        // to get the signature of every stratum.
        for (Pair<Integer, Node> pair : block.getStratumUsedPerNode()) {
            // Get the name of the tuple set and then access the strataPerTupleSet map to get its list of strata.
            String tupleSetName = pair.getRight().getTupleSet().getTable().getName();
            Stratum stratum = this.strataPerTupleSet.get(tupleSetName).get(pair.getLeft());
            signatures.add(stratum.getSignature());
        }

        return signatures;
    }

}
