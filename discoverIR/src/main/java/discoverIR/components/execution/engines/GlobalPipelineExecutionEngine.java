package discoverIR.components.execution.engines;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;
import discoverIR.components.execution.executors.CandidateNetworkExecutor;
import discoverIR.components.execution.executors.SinglePipelinedExecutor;
import discoverIR.exceptions.JoinCandidateNotFoundException;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.Node;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.TupleSet;


// Models a pair of a Candidate Network along with it's Maximum Possible Feature Score.
class CandidateNetworkMPFSPair extends Pair<JoiningNetworkOfTupleSets, Double> {

    static class ScoreComparator implements Comparator<CandidateNetworkMPFSPair> {
        @Override
        public int compare(CandidateNetworkMPFSPair a, CandidateNetworkMPFSPair b) {
            return b.getScore().compareTo(a.getScore());
        }
    }

    CandidateNetworkMPFSPair (JoiningNetworkOfTupleSets candidateNetwork, Double score) {
        super(candidateNetwork, score);
    }

    // Getters and setters.
    JoiningNetworkOfTupleSets getCandidateNetwork() {
        return this.getLeft();
    }

    Double getScore() {
        return this.getRight();
    }

    @Override
    public int hashCode() {
        int hash = 7;        
        hash = 31 * hash + ((this.getCandidateNetwork() != null) ? this.getCandidateNetwork().hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof CandidateNetworkMPFSPair)) return false;
        

        // Else compare the candidate networks
        JoiningNetworkOfTupleSets jnts = ((CandidateNetworkMPFSPair) obj).getCandidateNetwork();
        return this.getCandidateNetwork().equals(jnts);
    }
}

// Input  : A list of Candidate Networks C = {C1, ..., Cv}
// Output : Stream of joining trees of tuples that are the result of
//          the execution of the above Candidate Networks.
public class GlobalPipelineExecutionEngine extends ExecutionEngine {

    List<TupleSet> allNonFreeTupleSets; // All the non-free tuple sets produced by the IREngine.
    PriorityQueue<OverloadedTuple> results;    

    // A priority queue storing the networks prioritizing the ones 
    // with the highest Maximum Possible Feature Score.
    PriorityQueue<CandidateNetworkMPFSPair> networksPQueue;

    // A list of pointers to keep track of the prefixes (retrieved tuples S(TSi)) of every tuple set.
    // Initially all pointers are zero.
    private List<int[]> prefixesPerCN;

    int outputResultsCount; // Keeps track of the number of result that have been output.
    int retrievedTupleSets; // Keeps track of the number of tuple sets whose every tuple has been retrieved.

    private SchemaGraph schemaGraph;
    private SQLDatabase database;
    
    // Public Constructor.
    public GlobalPipelineExecutionEngine(List<JoiningNetworkOfTupleSets> networks, List<TupleSet> allNonFreeTupleSets,
            SchemaGraph schemaGraph, SQLDatabase database, Integer maxTuples, List<String> keywords, 
            boolean andSemantics, boolean printResultsOrderedByTable) {
        super(networks, maxTuples, keywords, andSemantics, printResultsOrderedByTable);        
        this.outputResultsCount = 0;
        this.retrievedTupleSets = 0;
        this.schemaGraph = schemaGraph;
        this.database = database;

        this.results = new PriorityQueue<OverloadedTuple>(new OverloadedTuple.ScoreComparator());
        this.networksPQueue = new PriorityQueue<>(new CandidateNetworkMPFSPair.ScoreComparator());
        this.allNonFreeTupleSets = allNonFreeTupleSets;

        // Initialize prefixes.
        this.prefixesPerCN = new ArrayList<>();
        for (JoiningNetworkOfTupleSets network: networks) {
            this.prefixesPerCN.add(new int[network.getNonFreeTupleSets().size()]);
        }               
    }

    // Returns the top results up to the required number.
    // TODO: fill body of function.
    @Override
    public List<OverloadedTuple> getResults() {
        return new ArrayList<OverloadedTuple>();
    }

    @Override
    public List<OverloadedTuple> getAllResults() {
        return new ArrayList<OverloadedTuple>();
    }

    // Return the best MPFS from all the Candidate networks in the Priority queue as the Global MPFS.
    private Double getGlobalMPFS() {
        return this.networksPQueue.peek().getScore();
    }

        // Returns a list of tuple sets where every tuple set contains
    // only the top tuple of every non free tuple set in the network.
    // The returned list also contains the free tuple sets of the network in the list.
    private JoiningNetworkOfTupleSets createTupleSetsWithTopTuples(JoiningNetworkOfTupleSets network) {
        Queue<Pair<Node, Node>> queue = new LinkedList<>(); // For the breadth first traversal.

        // Create a map between tuple sets in the network and the new tuple sets
        HashMap<TupleSet, TupleSet> tupleSetMap = new HashMap<>();
        for (int i = 0; i < network.getNonFreeTupleSets().size(); i++) {
            TupleSet t = network.getNonFreeTupleSets().get(i);
            tupleSetMap.put(t, new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTopTuple()));
        }

        // Free tuple sets dont need to change.
        for (TupleSet ft: network.getFreeTupleSets())
            tupleSetMap.put(ft, ft);
        
        // Copy the network
        JoiningNetworkOfTupleSets networkCopy = new JoiningNetworkOfTupleSets( tupleSetMap.get(network.getRoot().getTupleSet()) );
        queue.add(new Pair<Node, Node>(network.getRoot(), networkCopy.getRoot()));
        while (!queue.isEmpty()) {
            Pair<Node, Node> pair = queue.remove();
            Node node = pair.getLeft();
            Node copy = pair.getRight();
            
            for (Node child : node.getChildren()) {                
                Node childCopy = new Node(tupleSetMap.get( child.getTupleSet()));
                copy.addChild(childCopy);
                queue.add(new Pair<Node, Node>(child, childCopy));
            }
        }

        return networkCopy;
    }

    // Returns a new network where every tuple set contains all of its retrieved tuples (S(TSi))
    // as indicated by its prefix, but the tuple set that corresponds to the chosen tuple set
    // contains only the next not retrieved tuple of the set.    
    private JoiningNetworkOfTupleSets createNetworkWithRetrievedTuples(JoiningNetworkOfTupleSets network, int[] prefixes, TupleSet chosenTupleSet) {        
        Queue<Pair<Node, Node>> queue = new LinkedList<>(); // For the breadth first traversal.

        // Create a map between tuple sets in the network and the new tuple sets
        HashMap<TupleSet, TupleSet> tupleSetMap = new HashMap<>();
        for (int i = 0; i < network.getNonFreeTupleSets().size(); i++) {
            TupleSet t = network.getNonFreeTupleSets().get(i);
            int prefix = prefixes[i];

            if (t.equals(chosenTupleSet)) {
                // We subtract one from the prefix because it was just incremented for the
                // chosen tuple set, and we need to get the last tuple from the prefix.
                tupleSetMap.put(t, new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTupleByIndex(prefix-1)));
            }
            else {
                tupleSetMap.put(t, new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTuplesUpToIndex(prefix)));
            }
        }

        // Free tuple sets dont need to change.
        for (TupleSet ft: network.getFreeTupleSets())
            tupleSetMap.put(ft, ft);
        
        // Copy the network
        JoiningNetworkOfTupleSets networkCopy = new JoiningNetworkOfTupleSets( tupleSetMap.get(network.getRoot().getTupleSet()) );
        queue.add(new Pair<Node, Node>(network.getRoot(), networkCopy.getRoot()));
        while (!queue.isEmpty()) {
            Pair<Node, Node> pair = queue.remove();
            Node node = pair.getLeft();
            Node copy = pair.getRight();

            for (Node child : node.getChildren()) {                
                Node childCopy = new Node( tupleSetMap.get( child.getTupleSet()));
                copy.addChild(childCopy);
                queue.add(new Pair<Node, Node>(child, childCopy));
            }
        }

        return networkCopy;
    }

    // Adds a list of tuples to the queue and sorts them based on their score.
    private void addTuplesToQueueAndSort(List<OverloadedTuple> tuples) {
        this.results.addAll(tuples);
    }

    // Prints the tuples with a score above the global MPFS value (and removes them from the queue).
    private void printResultsAboveThreshold(Double threshold) {
        while ((!this.results.isEmpty()) && (this.results.peek().getScore() >= threshold)) {
            System.out.println("\nOUTPUT\n" + this.results.poll());
            this.outputResultsCount++;
            if (this.outputResultsCount == this.maxTuples) return;
        }
    }

    private void executeAllCombinations(CandidateNetworkExecutor executor, JoiningNetworkOfTupleSets network, int[] prefixes, TupleSet chosenTupleSet) {
        // Create a list of tuple sets with the retrieved tuples of every non free tuple set (and add the free ones).
        JoiningNetworkOfTupleSets networkWIthRetrievedTuples = createNetworkWithRetrievedTuples(network, prefixes, chosenTupleSet);

        // System.out.println("Prefixed TupleSets and chosen tuple :");
        // for (TupleSet t : tupleSetsWithRetrievedTuples) {
        //     System.out.println(t.toAbbreviation()); t.print(true); System.out.println();
        // }

        // Execute the parameterized query for all combinations.        
        List<OverloadedTuple> resultTuples = executor.execute(networkWIthRetrievedTuples).getTupleList();
        // if (resultTuples == null || resultTuples.size() == 0) System.out.println("TUPLES DID NOT JOIN\n");
        // else System.out.println("TUPLES JOINED PRODUCING " + resultTuples.size() + " RESULTS\n");

        // Add the results to the queue (if the join produced any results).
        if (resultTuples != null) {
            this.addTuplesToQueueAndSort(resultTuples);
        }       
    }

    // Update the MPFS of the parameter network in the Priority Queue. If it 
    // is not contained in the queue add it.
    private void updateCandidateNetworksMPFS(JoiningNetworkOfTupleSets network, int[] prefixes) {
        // Compute the networks MPFS.
        Double networksMPFS = network.computeGlobalMaximumPossibleFutureScoreOnly(
            network.getNonFreeTupleSets(), prefixes
        );

        // Create a new pair with the updated score.
        CandidateNetworkMPFSPair pair = new CandidateNetworkMPFSPair(network, networksMPFS);

        // It the pair is stored remove it and add a new with the updated score.
        // The contains function looks only for equal Candidate Networks in the pair, not scores.
        if (this.networksPQueue.contains(pair)) {
            this.networksPQueue.remove(pair);            
        }
        
        // Add the a new pair with the updates score
        this.networksPQueue.add(pair);
    }
    

    // Initialize the results Queue by Executing a parameterized Query for each Network.
    // joining the top tuples of it's TupleSets. Also update the Network MPFS pair queue with the 
    // updated MPFS's of each Candidate Network.
    private void initializeQueue(CandidateNetworkExecutor executor) {
        // System.out.println("|--_--|\nInitializing queue by joining top tuples per Candidate Network.\n");

        // For each Candidate Network Execute a parameterized Query 
        // joining the top tuples of it's tupleSets.
        Double tempGlobalMPFS = Double.MIN_VALUE;
        for (int index = 0; index < this.candidateNetworks.size(); index ++) {
            // Get the candidate Network and it's TupleSets Prefixes Table.
            JoiningNetworkOfTupleSets network = this.candidateNetworks.get(index);
            int [] prefixes = this.prefixesPerCN.get(index);
            // System.out.println("---\nNetwork : " + network.toAbbreviation());

            // Create a list of tuple sets with the top tuples of every non free tuple set (and add the free ones).
            JoiningNetworkOfTupleSets networkWithTopTuples = createTupleSetsWithTopTuples(network);
            
            // Contains the maximum MPFS value (left) and the chosen tuple set that produced it (right).
            // Networks with an MPFSi above this value will be executed.            
            Pair<Double, TupleSet> pair = network.computeGlobalMaximumPossibleFutureScore(
                network.getNonFreeTupleSets(), prefixes
            );

            // System.out.println("maximum MPFS = " + pair.getKey() + "\n");
            // System.out.println("Top tuples are:"); for (TupleSet t : tupleSetsWithTopTuples) t.print(true);
            // System.out.println();

            // Execute the parameterized query for the top tuples to initialize the queue.            
            List<OverloadedTuple> resultTuples = executor.execute(networkWithTopTuples).getTupleList();
            // if (resultTuples == null  || resultTuples.size() == 0) System.out.println("TOP TUPLES DID NOT JOIN\n");
            // else System.out.println("TOP TUPLES JOINED PRODUCING " + resultTuples.size() + " RESULTS\n");

            // Add the results to the queue (if the join produced any results).
            if (resultTuples != null) {
                this.addTuplesToQueueAndSort(resultTuples);

                // Update the prefixes since one tuple was retrieved from every tuple set.
                for (int i = 0; i < prefixes.length; i++) {
                    prefixes[i]++;
                    if (prefixes[i] == network.getNonFreeTupleSets().get(i).getSize()) {
                        this.retrievedTupleSets++;
                    }
                }
            }

            // System.out.print("\nSizes: ");
            // for (TupleSet t : network.getNonFreeTupleSets()) System.out.print(t.getSize() + " ");
            // System.out.print("\nPrefixes: ");
            // for (int i = 0; i < prefixes.length; i++) System.out.print(prefixes[i] + " ");
            // System.out.println();           
            // Keep the best MPFS from all the Candidate networks, before updating the scores.

            if (tempGlobalMPFS < pair.getLeft())
                tempGlobalMPFS = pair.getLeft();

            // Update the networks MPFS and add it to the Priority queue.
            this.updateCandidateNetworksMPFS(network, prefixes);

            System.out.println();
        }

        // System.out.println("Global MPFS from initialization is: " + tempGlobalMPFS + "\n|--_--|\n");


        // Print any results came from the initialization.
        this.printResultsAboveThreshold(tempGlobalMPFS);                
    }

    // Returns true if there are tuples not "prefixed" in the 
    // tupleSets of all candidate networks.
    // A tuple if "prefixed" if it is added to the prefix Set of the TupleSet it is contained.
    public boolean areTuplesLeft() {
        int numberOfTuplesSets = 0;
        for (JoiningNetworkOfTupleSets network : this.candidateNetworks)
            numberOfTuplesSets += network.getNonFreeTupleSets().size();

        return this.retrievedTupleSets < numberOfTuplesSets;
    }

    @Override
    public void execute() {
        // JoiningNetworkOfTupleSets network = this.candidateNetworks.get(0);
        // System.out.println("\nCurrent network\n" + network + "\n");

        // // If the network only has one tuple set just print its top 'maxTuples' tuples.
        // if (this.nonFreeTupleSets.size() == 1) {
        //     this.nonFreeTupleSets.get(0).printTuplesUpToIndex(this.maxTuples);
        //     System.out.println("\n--------------------------\n");
        //     return;
        // }

        // Create and initialize an executor.
        CandidateNetworkExecutor executor = new CandidateNetworkExecutor(
            this.schemaGraph,
            this.database,
            this.allNonFreeTupleSets,
            this.maxTuples
        );

        executor.initializeExecutor();
    
    
        // Initialize the Queue of results by executing a parameterized query for 
        // every Candidate networks top tuples.
        this.initializeQueue(executor);
               

        // In each loop get the most promising candidate network
        // and execute a minimal "proportion" of that network.
        while ((this.outputResultsCount < this.maxTuples) && this.areTuplesLeft()) {
            // Get the network with the Best MPFS score.
            CandidateNetworkMPFSPair promisingNetworkPair = this.networksPQueue.peek();
            JoiningNetworkOfTupleSets network = promisingNetworkPair.getCandidateNetwork();
            int [] prefixes = this.prefixesPerCN.get(this.candidateNetworks.indexOf(network));

            // System.out.println("----\nBest network : " + network.toAbbreviation() +
            //     "\nWith Score : " + promisingNetworkPair.getScore() +
            //     "\nGlobalMPFS : " + this.getGlobalMPFS() + "\n"
            // );
            
            // First Compute the maximum possible feature score of the Network.
            Pair<Double, TupleSet> pair = network.computeGlobalMaximumPossibleFutureScore(network.getNonFreeTupleSets(), prefixes);

            // Increment the prefix of the chosen tuple set by one.            
            int chosenTupleSetIndex = network.getNonFreeTupleSets().indexOf(pair.getRight());
            prefixes[chosenTupleSetIndex]++;
            if (prefixes[chosenTupleSetIndex] == pair.getRight().getSize()) {
                this.retrievedTupleSets++;
            }

            // System.out.print("\nSizes: ");
            // for (TupleSet t : network.getNonFreeTupleSets()) System.out.print(t.getSize() + " ");
            // System.out.print("\nPrefixes: ");
            // for (int i = 0; i < prefixes.length; i++) System.out.print(prefixes[i] + " ");
            // System.out.println("\nChosen: " + pair.getValue().toAbbreviation());
            // System.out.println("maximum MPFS = " + pair.getKey() + "\n");

            // Execute the combinations.
            this.executeAllCombinations(executor, network, prefixes, pair.getRight());

            // Print any results before updating the scores.
            this.printResultsAboveThreshold(this.getGlobalMPFS());

            // Update the networks MPFS in the Priority Queue.
            this.updateCandidateNetworksMPFS(network, prefixes);
            
            // System.out.println("\nUpdated GlobalMPFS : " + this.getGlobalMPFS() + "\n");
        }

        executor.finalizeExecutor();

        System.out.println("\n--------------------------\n");
    }


    

}

