package discoverIR.components.execution.engines;

import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import shared.util.Pair;

import discoverIR.model.TupleSet;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.components.execution.executors.SinglePipelinedExecutor;
import discoverIR.model.OverloadedTuple;
import discoverIR.exceptions.JoinCandidateNotFoundException;

import java.util.List;
import java.util.ArrayList;
import java.util.PriorityQueue;

// Input: A candidate network C and the non-free tuple sets (TSi, ..., TSu) of the network.
// Output: Stream of joining trees of tuples.
public class SinglePipelinedExecutionEngine extends ExecutionEngine {

    private List<TupleSet> nonFreeTupleSets; // All the non-free tuple sets produced by the IREngine.
    PriorityQueue<OverloadedTuple> results;

    // A list of pointers to keep track of the prefixes (retrieved tuples S(TSi)) of every tuple set.
    // Initially all pointers are zero.
    private int[] prefixes;

    int outputResultsCount; // Keeps track of the number of result that have been output.
    int retrievedTupleSets; // Keeps track of the number of tuple sets whose every tuple has been retrieved.

    private SchemaGraph schemaGraph;
    private SQLDatabase database;

    public SinglePipelinedExecutionEngine(JoiningNetworkOfTupleSets network, List<TupleSet> nonFreeTupleSets,
            SchemaGraph schemaGraph, SQLDatabase database, Integer maxTuples,
            List<String> keywords, boolean andSemantics) {
        super(network, maxTuples, keywords, andSemantics);
        this.nonFreeTupleSets = nonFreeTupleSets;
        this.results = new PriorityQueue<OverloadedTuple>(new OverloadedTuple.ScoreComparator());
        this.outputResultsCount = 0;
        this.retrievedTupleSets = 0;
        this.prefixes = new int[nonFreeTupleSets.size()];
        this.schemaGraph = schemaGraph;
        this.database = database;
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

    // Returns a list of tuple sets where every tuple set contains
    // only the top tuple of every non free tuple set in the network.
    // The returned list also contains the free tuple sets of the network in the list.
    private List<TupleSet> createTupleSetsWithTopTuples() {
        List<TupleSet> tupleSets = new ArrayList<TupleSet>();

        for (TupleSet t : this.nonFreeTupleSets) {
            tupleSets.add(new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTopTuple()));
        }

        // Add the free tuple sets needed for the joins later.
        tupleSets.addAll(this.candidateNetworks.get(0).getFreeTupleSets());

        return tupleSets;
    }

    // Returns a list of tuple sets where every tuple set contains all of its retrieved tuples (S(TSi))
    // as indicated by its prefix, but the tuple set that corresponds to the chosen tuple set
    // contains only the next not retrieved tuple of the set.
    // The returned list also contains the free tuple sets of the network in the list.
    private List<TupleSet> createTupleSetsWithRetrievedTuples(TupleSet chosenTupleSet) {
        List<TupleSet> tupleSets = new ArrayList<TupleSet>();

        for (int i = 0; i < this.nonFreeTupleSets.size(); i++) {
            TupleSet t = this.nonFreeTupleSets.get(i);
            int prefix = this.prefixes[i];

            if (t.equals(chosenTupleSet)) {
                // We subtract one from the prefix because it was just incremented for the
                // chosen tuple set, and we need to get the last tuple from the prefix.
                tupleSets.add(new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTupleByIndex(prefix-1)));
            }
            else {
                tupleSets.add(new TupleSet(t.getTable(), t.getColumnsContainingKeywords(), t.getTuplesUpToIndex(prefix)));
            }
        }

        // Add the free tuple sets needed for the joins later.
        tupleSets.addAll(this.candidateNetworks.get(0).getFreeTupleSets());

        return tupleSets;
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

    private void executeAllCombinations(SinglePipelinedExecutor executor, TupleSet chosenTupleSet) {
        // Create a list of tuple sets with the retrieved tuples of every non free tuple set (and add the free ones).
        List<TupleSet> tupleSetsWithRetrievedTuples = createTupleSetsWithRetrievedTuples(chosenTupleSet);

        for (TupleSet t : tupleSetsWithRetrievedTuples) {
            System.out.println(t.toAbbreviation()); t.print(true); System.out.println();
        }

        // Execute the parametrised query for all combinations.
        try {
            List<OverloadedTuple> resultTuples = executor.execute(tupleSetsWithRetrievedTuples);
            if (resultTuples == null || resultTuples.size() == 0) System.out.println("TUPLES DID NOT JOIN\n");
            else System.out.println("TUPLES JOINED PRODUCING " + resultTuples.size() + " RESULTS\n");

            // Add the results to the queue (if the join produced any results).
            if (resultTuples != null) {
                this.addTuplesToQueueAndSort(resultTuples);
            }

        }
        catch (JoinCandidateNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute() {
        JoiningNetworkOfTupleSets network = this.candidateNetworks.get(0);
        System.out.println("\nCurrent network\n" + network + "\n");

        // If the network only has one tuple set just print its top 'maxTuples' tuples.
        if (this.nonFreeTupleSets.size() == 1) {
            this.nonFreeTupleSets.get(0).printTuplesUpToIndex(this.maxTuples);
            System.out.println("\n--------------------------\n");
            return;
        }

        // Create and initialize an executor.
        SinglePipelinedExecutor executor = new SinglePipelinedExecutor(
                this.database,
                this.schemaGraph,
                this.maxTuples,
                this.nonFreeTupleSets
        );

        executor.initializeExecutor();

        System.out.println("Initializing queue by joining top tuples.\n");

        // Create a list of tuple sets with the top tuples of every non free tuple set (and add the free ones).
        List<TupleSet> tupleSetsWithTopTuples = createTupleSetsWithTopTuples();

        // Contains the maximum MPFS value (left) and the chosen tuple set that produced it (right).
        // Networks with an MPFSi above this value will be executed.
        Pair<Double, TupleSet> pair = network.computeGlobalMaximumPossibleFutureScore(
                this.nonFreeTupleSets, this.prefixes);

        System.out.println("maximum MPFS = " + pair.getLeft() + "\n");
        System.out.println("Top tuples are:"); for (TupleSet t : tupleSetsWithTopTuples) t.print(true);
        System.out.println();

        // Execute the parameterized query for the top tuples to initialize the queue.
        try {
            List<OverloadedTuple> resultTuples = executor.execute(tupleSetsWithTopTuples);
            if (resultTuples == null  || resultTuples.size() == 0) System.out.println("TOP TUPLES DID NOT JOIN\n");
            else System.out.println("TOP TUPLES JOINED PRODUCING " + resultTuples.size() + " RESULTS\n");

            // Add the results to the queue (if the join produced any results).
            if (resultTuples != null) {
                this.addTuplesToQueueAndSort(resultTuples);
                this.printResultsAboveThreshold(pair.getLeft());

                // Update the prefixes since one tuple was retrieved from every tuple set.
                for (int i = 0; i < this.prefixes.length; i++) {
                    this.prefixes[i]++;
                    if (this.prefixes[i] == this.nonFreeTupleSets.get(i).getSize()) {
                        this.retrievedTupleSets++;
                    }
                }
            }

            System.out.print("\nSizes: ");
            for (TupleSet t : this.nonFreeTupleSets) System.out.print(t.getSize() + " ");
            System.out.print("\nPrefixes: ");
            for (int i = 0; i < this.prefixes.length; i++) System.out.print(this.prefixes[i] + " ");
            System.out.println();
        }
        catch (JoinCandidateNotFoundException e) {
            e.printStackTrace();
        }

        while ((this.outputResultsCount < this.maxTuples) && (this.retrievedTupleSets < this.nonFreeTupleSets.size())) {
            pair = network.computeGlobalMaximumPossibleFutureScore(this.nonFreeTupleSets, this.prefixes);

            // Increment the prefix of the chosen tuple set by one.
            this.prefixes[this.nonFreeTupleSets.indexOf(pair.getRight())]++;
            if (this.prefixes[this.nonFreeTupleSets.indexOf(pair.getRight())] == pair.getRight().getSize()) {
                this.retrievedTupleSets++;
            }

            System.out.print("\nSizes: ");
            for (TupleSet t : this.nonFreeTupleSets) System.out.print(t.getSize() + " ");
            System.out.print("\nPrefixes: ");
            for (int i = 0; i < this.prefixes.length; i++) System.out.print(this.prefixes[i] + " ");
            System.out.println("\nChosen: " + pair.getRight().toAbbreviation());
            System.out.println("maximum MPFS = " + pair.getLeft() + "\n");

            this.executeAllCombinations(executor, pair.getRight());
            this.printResultsAboveThreshold(pair.getLeft());
        }

        executor.finalizeExecutor();

        System.out.println("\n--------------------------\n");
    }

}
