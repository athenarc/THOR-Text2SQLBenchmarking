package spark.components;

import spark.model.JoiningNetworkOfTupleSets;
import spark.model.TupleSetGraph;
import spark.model.TupleSet;
import spark.model.AdjacentTupleSets;

import shared.connectivity.thor.response.Table;

import java.util.Queue;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

// Input: Set of m keywords {k1, ..., km}, non-empty tuple sets Ri^Q, and the max candidate network size.
// Output: A complete and non-redundant set of candidate joining networks of tuple sets.
public class CandidateNetworksGenerator {

    // A class keeping a Queue and a HashSet of JoiningNetworkOfTupleSets both containing the same networks.
    // This parallel structure helps us store only unique candidate Networks.
    class UniqueCandidateNetworkQueue {

        private Queue<JoiningNetworkOfTupleSets> networkQueue; // A queue of 'active' networks.
        private Set<JoiningNetworkOfTupleSets> setOfUniqueNetworks; // A set containing unique networks.

        // Statistics.
        public int discardedNetworks;
        public int networksAddedInQueue;

        UniqueCandidateNetworkQueue() {
            this.networkQueue = new LinkedList<>();
            this.setOfUniqueNetworks = new HashSet<>();
            this.discardedNetworks = 0;
            this.networksAddedInQueue = 0;
        }

        // Prints the statistics.
        public void printStats() {
            System.out.println("Networks added in Active Networks Queue: " + this.networksAddedInQueue);
            System.out.println("Discarded duplicate Networks: " + this.discardedNetworks);
        }       

        // Returns true if the queue empty.
        boolean isEmpty() {
            return networkQueue.isEmpty();
        }

        // Adds a JoiningNetworkOfTupleSets if it is not contained already.
        // Returns true or false depending on if the network was added in the Unique Queue.
        boolean add(JoiningNetworkOfTupleSets network) {
            // Check if we have the network in the HashSet.
            if (this.setOfUniqueNetworks.contains(network)) {
                // System.out.println("#####\nNetwork :\n" + network + "\nIs already in the Queue!\n####");
                this.discardedNetworks++;
                return false;
            }

            // Else add it to the queue and the set.
            this.networkQueue.add(network);
            this.setOfUniqueNetworks.add(network);
            this.networksAddedInQueue++;
            // System.out.println("Added\n");

            return true;
        }

        // Removes and returns the JoiningNetworkOfTupleSets that is at the end of the queue.
        JoiningNetworkOfTupleSets remove() {
            // Remove the network form the Queue and the HashSet.
            JoiningNetworkOfTupleSets network = this.networkQueue.remove();
            this.setOfUniqueNetworks.remove(network);

            return network;
        }

    }

    private List<JoiningNetworkOfTupleSets> candidateNetworks; // The output of the Candidate Network Generator component.
    private List<TupleSet> tupleSets; // The output of the IREngine component.
    private List<String> keywords; // The keywords of the query.
    private int maxSize; // Max size of a candidate network.

    private TupleSetGraph tupleSetGraph;
    private UniqueCandidateNetworkQueue networkQueue; // A queue of 'active' networks.

    // Stats 
    private int networksProcessed;

    public CandidateNetworksGenerator(List<TupleSet> tupleSets, TupleSetGraph tupleSetGraph,
            List<String> keywords, int maxSize) {
        this.candidateNetworks = new ArrayList<JoiningNetworkOfTupleSets>();
        this.tupleSets = new ArrayList<TupleSet>();
        this.tupleSets.addAll(tupleSets);
        this.keywords = keywords;
        this.maxSize = maxSize;
        this.networksProcessed = 0;

        this.tupleSetGraph = tupleSetGraph;
        this.networkQueue = new UniqueCandidateNetworkQueue();
    }

    // Prints the statistics.
    public void printStats() {
        this.networkQueue.printStats();
        System.out.println("Candidate Networks: " + this.candidateNetworks.size());
    }

    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {
        List<Table.Row> rows = new ArrayList<>();  // The table rows.
                        
        rows.addAll( Arrays.asList(            
            new Table.Row( Arrays.asList(                
                "Max network size",
                Integer.toString(this.maxSize)
            )),
            new Table.Row( Arrays.asList(                
                "All networks created",
                Integer.toString(this.networksProcessed)
            )),
            new Table.Row( Arrays.asList(                
                "Discarded duplicate networks",
                Integer.toString(this.networkQueue.discardedNetworks)
            )),
            new Table.Row( Arrays.asList(                
                "Valid networks",
                Integer.toString(this.candidateNetworks.size())
            ))
        ));
                
        // Return the table containing the Components Info.
        return new Table(rows);        
    }

    // Getters and Setters.
    public List<JoiningNetworkOfTupleSets> getCandidateNetworks() {
        return candidateNetworks;
    }

    // Initializes the queue by pushing a candidate network for every non-free tuple set.
    private void initializeQueue() {
        if (!this.networkQueue.isEmpty()) return;

        for (TupleSet tupleSet : this.tupleSets) {
            this.networkQueue.add(new JoiningNetworkOfTupleSets(tupleSet));
            this.networksProcessed++;
            // System.out.print("Added JNTS with root: " + tupleSet.toAbbreviation());
            // System.out.println(" contains keywords: " + tupleSet.getKeywords());
        }

        // System.out.println();
    }

    // The main function of the Candidate Network Generator component.
    // Generates and returns the candidate joining networks of tuple sets.
    public List<JoiningNetworkOfTupleSets> generate(boolean andSemantics) {
        this.initializeQueue(); // Creates a network for every non free tuple set.

        while (!this.networkQueue.isEmpty()) {
            // Get the head of the queue.
            JoiningNetworkOfTupleSets network = networkQueue.remove();
            // System.out.println("==============\n\nCurrent JNTS\n" + network);

            // Check the pruning and acceptance conditions.
            if (network.violatesPruningCondition()) {
                // System.out.println("Violates pruning condition.");
                continue;
            }

            if (network.satisfiesAcceptanceConditions(this.keywords, andSemantics)) {
                this.candidateNetworks.add(network);
            }

            // Get the adjacent tuple sets of every node of the network.
            List<AdjacentTupleSets> adjacentTupleSetsList = network.getAdjacentTupleSets(this.tupleSetGraph);
            // for (AdjacentTupleSets a : adjacentTupleSetsList) System.out.print("Adjacent of " + a);
            // System.out.println("\n--------------\n"); System.out.flush();

            // Expand the network by attaching every node with its adjacent tuple sets.
            for (int i = 0; i < adjacentTupleSetsList.size(); i++) {
                AdjacentTupleSets adjacentTupleSets = adjacentTupleSetsList.get(i);

                for (TupleSet adjacent : adjacentTupleSets.getAdjacentTupleSets()) {
                    // System.out.print("For " + adjacentTupleSets.getTupleSet().toAbbreviation() + "\n");
                    // System.out.println(" check expansion rule for adjacent: " + adjacent.toAbbreviation() + " ");
                    // Check the size of the network and the expansion rule.
                    if (network.getSize() < maxSize) {
                        // Connect the two tuple sets in the network.
                        JoiningNetworkOfTupleSets expandedJnts = network.expand(adjacentTupleSets.getTupleSet(),
                                adjacent, i, this.tupleSetGraph);

                        // System.out.println("Expand with Ri^K: " + adjacent.toAbbreviation());
                        // System.out.println("Expanded JNTS:\n" + expandedJnts);

                        this.networkQueue.add(expandedJnts);
                        this.networksProcessed++;
                    }
                    else {
                        // System.out.println("Ignore Ri^K: " + adjacent.toAbbreviation());
                    }
                    // System.out.println();
                }
                // System.out.println();
            }
        }

        return this.candidateNetworks;
    }

}
