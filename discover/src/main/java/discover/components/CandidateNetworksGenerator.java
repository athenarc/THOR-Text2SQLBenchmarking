package discover.components;

import discover.model.JoiningNetworkOfTupleSets;
import discover.model.TupleSetGraph;
import shared.connectivity.thor.response.Table;
import shared.util.Timer;
import discover.model.TupleSet;
import discover.model.AdjacentTupleSets;

import java.util.Queue;
import java.util.Set;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

// Input: Set of m keywords {k1, ..., km}, non-empty Ri^k tuple sets, and the max candidate network size.
// Output: A complete and non-redundant set of candidate joining networks of tuple sets.
public class CandidateNetworksGenerator {

    private List<TupleSet> tupleSets; // The output of the post processor component.
    private List<String> keywords; // The keywords of the query.
    private int maxSize; // Max size (number of joins) of a candidate network.

    private TupleSetGraph tupleSetGraph;
    private UniqueCandidateNetworkQueue jntsQueue; // A queue of "active" JNTSs.

    // The output of the Candidate Network Generator component as described above.
    private List<JoiningNetworkOfTupleSets> candidateNetworks;

    // Statistics
    public int networksProcessed;
    public double timeGeneratingNetworks;

    public CandidateNetworksGenerator(List<TupleSet> tupleSets, TupleSetGraph tupleSetGraph,
            List<String> keywords, int maxSize) {
        this.tupleSets = new ArrayList<TupleSet>();
        this.tupleSets.addAll(tupleSets);
        this.keywords = keywords;
        this.maxSize = maxSize;
        this.networksProcessed = 0;

        this.tupleSetGraph = tupleSetGraph;
        this.jntsQueue = new UniqueCandidateNetworkQueue();
        this.candidateNetworks = new ArrayList<JoiningNetworkOfTupleSets>();
    }

    // Getters and Setters.
    public List<JoiningNetworkOfTupleSets> getCandidateNetworks() {
        return candidateNetworks;
    }
   
    // The main function of the Candidate Network Generator component.
    // Generates the candidate joining networks of tuple sets.
    public void generateCandidateNetworks() {
        // Start the timer.
        Timer timer = new Timer();
        timer.start();

        // Initialize the queue with the tuple sets of a random keyword.
        this.randomlyInitializeQueue();

        while (!this.jntsQueue.isEmpty()) {
            // Get the head of the queue.
            JoiningNetworkOfTupleSets jnts = jntsQueue.remove();
            // System.out.println("==============\nCurrent JNTS\n" + jnts);

            // Check the conditions.
            if (jnts.violatesPruningCondition()) {
                continue;
            }
            else if (jnts.satisfiesAcceptanceConditions(this.keywords)) {
                this.candidateNetworks.add(jnts);
            }
            else {
                // Get the adjacent tuple sets of every node of the network.
                List<AdjacentTupleSets> adjacentTupleSetsList = jnts.getAdjacentTupleSets(this.tupleSetGraph);

                // System.out.println("AdjacentTupleSets: ");
                // for (AdjacentTupleSets a : adjacentTupleSetsList) System.out.print(a);
                // System.out.println("\n-----\n"); System.out.flush();

                // Check the expansion rule for every adjacent tuple set of the network.
                for (int i = 0; i < adjacentTupleSetsList.size(); i++) {
                    AdjacentTupleSets adjacentTupleSets = adjacentTupleSetsList.get(i);
                    // System.out.println("For: " + adjacentTupleSets.getTupleSet().toAbbreviation());

                    for (TupleSet adjacent : adjacentTupleSets.getAdjacentTupleSets()) {
                        // System.out.println("check expansion rule for adjacent: " + adjacent.toAbbreviation() + " ");

                        // Check the size of the network and the expansion rule.
                        if ((jnts.getSize() < maxSize) && (jnts.checkExpansionRule(adjacent))) {
                            // Connect the two tuple sets in the network.
                            JoiningNetworkOfTupleSets expandedJnts = jnts.expand(
                                adjacentTupleSets.getTupleSet(), adjacent, i, this.tupleSetGraph
                            );

                            // System.out.println("Expand with Ri^K: " + adjacent.toAbbreviation());
                            // System.out.println("Expanded JNTS:\n" + expandedJnts);

                            if (expandedJnts.satisfiesLeavesCondition(this.keywords.size())) {
                                this.jntsQueue.add(expandedJnts);
                                this.networksProcessed++;
                            }
                        }
                        else {
                            // System.out.println("Ignore Ri^K: " + adjacent.toAbbreviation());
                        }
                        // System.out.println();
                    }
                    // System.out.println();
                }
            }
        }

        this.timeGeneratingNetworks = timer.stop();
    }


     // Initialize the queue with every JNTS that contains
    // a single tuple set containing a randomly picked keyword.
    public void randomlyInitializeQueue() {
        if (!this.jntsQueue.isEmpty()) {
            return;
        }

        // Extract a random keyword.
        // Random random = new Random();
        String randomKeyword = this.keywords.get(this.keywords.size() -1); //random.nextInt(this.keywords.size()));

        // System.out.println("Initializing the queue.");
        // System.out.println("Random keyword: " + randomKeyword);

        // Create a JNTS for every tuple set containing the random keyword.
        for (TupleSet tupleSet : this.tupleSets) {
            if (tupleSet.containsKeyword(randomKeyword)) {

                this.jntsQueue.add(new JoiningNetworkOfTupleSets(tupleSet));
                this.networksProcessed++;

                // System.out.println("Added JNTS with root tuple set: " + tupleSet.toAbbreviation());
            }
        }

        // System.out.println();
    }


    // Print statistics.
    public void printStats() {
        System.out.println("CANDIDATE NETWORK GENERATOR STATS:");
        System.out.println("\tNumber of Networks processed: " + this.networksProcessed);
        System.out.println("\tTime to generate Candidate Networks: " + this.timeGeneratingNetworks);
        this.jntsQueue.printStats();
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
                Integer.toString(this.jntsQueue.networksAddedInQueue)
            )),
            new Table.Row( Arrays.asList(                
                "Discarded duplicate networks",
                Integer.toString(this.jntsQueue.discardedNetworks)
            )),
            new Table.Row( Arrays.asList(                
                "Valid networks",
                Integer.toString(this.candidateNetworks.size())
            ))
        ));
                
        // Return the table containing the Components Info.
        return new Table(rows);        
    }

}


// A class keeping a queue and a HashSet of JoiningNetworkOfTupleSets
// bothContaining the same networks. This parallel structure helps us
// store only unique candidate Networks.
class UniqueCandidateNetworkQueue {
    private Queue<JoiningNetworkOfTupleSets> networkQueue; // A queue of 'active' networks.
    private Set<JoiningNetworkOfTupleSets> setOfUniqueNetworks; // A set containing unique networks.

    // Statistics.
    public int discardedNetworks;
    public int networksAddedInQueue;


    // Constructor.
    UniqueCandidateNetworkQueue() {
        this.networkQueue = new LinkedList<>();
        this.setOfUniqueNetworks = new HashSet<>();
        this.discardedNetworks = 0;
        this.networksAddedInQueue = 0;
    }   

    // Return true if the queue empty?
    boolean isEmpty() {
        return networkQueue.isEmpty();
    }

    // Adds a JoiningNetworkOfTupleSets if it is not contained already.
    // Returns true or false depending on if the network was added in
    // the Unique Queue.
    boolean add(JoiningNetworkOfTupleSets network) {
        this.networksAddedInQueue++;

        // Check if we have the network in the hashSet.
        if (this.setOfUniqueNetworks.contains(network)) {
            this.discardedNetworks++;
            return false;
        }

        // Else add it to the queue and the set.
        this.networkQueue.add(network);
        this.setOfUniqueNetworks.add(network);        
        // System.out.println("####Added\n");

        // Return true.
        return true;
    }

    // Removes and returns the JoiningNetworkOfTupleSets that is
    // at the end of the queue.
    JoiningNetworkOfTupleSets remove() {
        // Remove the network form the queue.
        JoiningNetworkOfTupleSets network = this.networkQueue.remove();

        // Remove the network from the HashSet.
        this.setOfUniqueNetworks.remove(network);

        // Return the network.
        return network;
    }

     // Print the statistics.
     public void printStats() {
        System.out.println("\tNetworks added in Active Networks Queue: " + this.networksAddedInQueue);
        System.out.println("\tDiscarded duplicate Networks: " + this.discardedNetworks);
    }

}