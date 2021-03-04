package discoverIR.components.execution.engines;

import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;

import discoverIR.components.execution.executors.CandidateNetworkExecutor;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.OverloadedTupleList;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.TupleSet;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

// This execution engine implements the functionality of the Sparse excution algorithm.
public class SparseExecutionEngine extends ExecutionEngine {

    // A Schema Graph Containing the database Tables and all the Temp
    // Tables (representing Intermediate Results) that will be created while executing this Plan.
    private SchemaGraph modifiedSchemaGraph;

    // The only modification applied in the Database created in the begging of
    // the program will be the addition of Temp Tables containing then
    // the intermediate results.
    // At the end of the Executor all the Temp Tables  will be dropped.
    private SQLDatabase modifiedDatabase;

    private List<TupleSet> nonFreeTupleSets; // All the non-free tuple sets produced by the IREngine.
    private List<OverloadedTuple> results; // List to save the results.

    public SparseExecutionEngine(List<JoiningNetworkOfTupleSets> candidateNetworks, SchemaGraph schemaGraph,
            SQLDatabase database, List<TupleSet> nonFreeTupleSets, Integer maxTuples, List<String> keywords,
            boolean andSemantics, boolean printResultsOrderedByTable, boolean efficientPlanGenerator) {
        super(candidateNetworks, maxTuples, keywords, andSemantics, printResultsOrderedByTable);
        this.modifiedSchemaGraph = new SchemaGraph(schemaGraph);
        this.modifiedDatabase = database;
        this.nonFreeTupleSets = nonFreeTupleSets;
    }

    // Returns the top results up to the required number.
    @Override
    public List<OverloadedTuple> getResults() {
        if (this.results.size() > super.maxTuples) {
            return this.results.subList(0, super.maxTuples);
        }
        else {
            return this.results;
        }
    }

    @Override
    public List<OverloadedTuple> getAllResults() {
        return this.results;
    }


    // Given a list of networks, this function returns the maximum network size in the list.
    private int getMaxNetworkSize(List<JoiningNetworkOfTupleSets> networks) {
        int maxSize = 0;

        for (JoiningNetworkOfTupleSets network : networks) {
            int size = network.getSize();
            if (size > maxSize) {
                maxSize = size;
            }
        }

        return maxSize;
    }

    // Given a list of networks, this function groups the networks in a two-level list,
    // where the first level corresponds to the network size. For example,
    // at position '0' it contains a list of networks of size 0 (number of joins), etc..
    private List<List<JoiningNetworkOfTupleSets>> groupNetworksBySize(List<JoiningNetworkOfTupleSets> networks) {
        // Get the size of the biggest network.
        int maxSize = getMaxNetworkSize(networks);

        // Create a list with maxSize inner lists (one for every possible network size).
        List<List<JoiningNetworkOfTupleSets>> groupedNetworks = new ArrayList<List<JoiningNetworkOfTupleSets>>(maxSize+1);
        for (int i = 0; i < maxSize+1; i++) {
            groupedNetworks.add(new ArrayList<JoiningNetworkOfTupleSets>());
        }

        // Group the networks in the inner lists according to their size.
        for (JoiningNetworkOfTupleSets network : networks) {
            groupedNetworks.get(network.getSize()).add(network);
        }

        return groupedNetworks;
    }

    @Override
    public void execute() {
        // Create a Candidate Network Executor.
        CandidateNetworkExecutor candidateNetworkExecutor = new CandidateNetworkExecutor(
            this.modifiedSchemaGraph,
            this.modifiedDatabase,
            this.nonFreeTupleSets,
            this.maxTuples
        );

        // Initialize the Candidate Network Executor.
        candidateNetworkExecutor.initializeExecutor();

        // Group the candidate networks according to their size.
        // Smaller networks need to be evaluated first for efficiency reasons.
        List<List<JoiningNetworkOfTupleSets>> groupedNetworks = groupNetworksBySize(this.candidateNetworks);

        List<OverloadedTupleList> topTuples = new ArrayList<OverloadedTupleList>();
        Double threshold = 0.0; // Networks with an MPSi above this value will be executed.
        int topTuplesNumber = 0;

        // Loop through the networks (smaller first).
        for (List<JoiningNetworkOfTupleSets> innerList : groupedNetworks) {
            for (JoiningNetworkOfTupleSets network : innerList) {
                Double networkScore = network.computeMaximumPossibleScore();

                if (topTuplesNumber < this.maxTuples || networkScore > threshold) {
                    // System.out.println("EXECUTE " + network);
                    // System.out.println("passes threshold: " + threshold + " with MPS: " + networkScore + "\n");

                    // Execute the network.
                    OverloadedTupleList resultTuples = candidateNetworkExecutor.execute(network);

                    // Truncate the result tuples depending on the semantics
                    if (this.andSemantics) {
                        resultTuples.truncate(this.keywords);
                    }

                    // If the result is empty then continue the loop.
                    // An empty result indicates that the CN is already executed.
                    if (resultTuples.isEmpty()) continue;

                    // System.out.println("\nRESULT\n");
                    // resultTuples.print(true);

                    // Add the tuples and update the threshold
                    topTuples.add(resultTuples);
                    threshold = keepTopKTuples(topTuples, threshold);

                    // Update the capacity of the topTuplesList;
                    for (OverloadedTupleList list: topTuples)
                        topTuplesNumber += list.getTupleList().size();

                    // System.out.println("\nTOP TUPLES FROM ALL CNs\n");
                    // for (OverloadedTupleList list: topTuples)  {
                    //     list.print(true);
                    // }
                    // System.out.println("\n--------------------\n");
                }
                // else {
                //     System.out.println(network);
                //     System.out.println("does not pass threshold: " + threshold + " with MPS: " + networkScore + "\n");
                // }
            }
        }

        // Create a list containing all top tuples.
        for (OverloadedTupleList tupleList: topTuples) {
            this.results.addAll(tupleList.getTupleList());
        }

        // Sort the list.
        Collections.sort(this.results, new OverloadedTuple.ScoreComparator());

        // Print the results.
        this.printResults(topTuples);

        // Finalize the Candidate Network Executor.
        candidateNetworkExecutor.finalizeExecutor();
    }


    // Prints the result tuples.
    private void printResults(List<OverloadedTupleList> topTuples) {
        // Depending on the printResultsOrderedByTable boolean value we
        // will print the tuples ordered by the table they belong to or as
        // single tuples.
        System.out.println("Sparse Execution Engine's Top " + this.maxTuples + " results\n");
        int resultsNumber = 0;
        if (this.printResultsOrderedByTable) {
            // Print the results                        
            for (OverloadedTupleList list: topTuples)  {
                list.print(true);
                System.out.println();
                resultsNumber += list.getTupleList().size();
            }            
        }
        else {
            // Create a list containing all tuples.
            List<OverloadedTuple> resultTuples = new ArrayList<>();
            for(OverloadedTupleList tupleList: topTuples) {
                resultTuples.addAll(tupleList.getTupleList());
            }

            // Sort the list.
            Collections.<OverloadedTuple>sort(resultTuples, new OverloadedTuple.ScoreComparator());

            // Keep only the maxTuples tuples.
            if (resultTuples.size() > this.maxTuples) {
                resultTuples = resultTuples.subList(0, this.maxTuples);
            }            

             // Print the results            
            for (OverloadedTuple tuple: resultTuples)  {
                System.out.println(tuple);
            }

            resultsNumber = resultTuples.size();
        }

       
        if (resultsNumber == 0) {
            System.out.println("(Empty Set)");
        }
        else if (resultsNumber < this.maxTuples) {
            System.out.println("(No more than " + resultsNumber + " result tuples were accepted)");
        }
    }

}
