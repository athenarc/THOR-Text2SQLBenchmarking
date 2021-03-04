package discoverIR.components.execution.engines;

import discoverIR.model.OverloadedTuple;
import discoverIR.model.OverloadedTupleList;
import shared.connectivity.thor.response.Table;
import discoverIR.model.JoiningNetworkOfTupleSets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Inputs: A list of Candidate Networks.
// Outputs: A list of OverloadedTuples.
//
// The Execution engine class is responsible for using the candidate
// networks to create a list of OverloadedTuples. Those OverloadedTuples
// will be the DiscoverIR System's results.
public abstract class ExecutionEngine {

    protected List<JoiningNetworkOfTupleSets> candidateNetworks;
    protected Integer maxTuples; // The maximum number of Results.
    protected List<String> keywords; // The keywords of the query.
    protected boolean andSemantics; // If true then the results must contain all keyword.
    protected boolean printResultsOrderedByTable; // If true print results Ordered by tables.

    public ExecutionEngine(List<JoiningNetworkOfTupleSets> candidateNetworks, Integer maxTuples,
                List<String> keywords, boolean andSemantics, boolean printResultsOrderedByTable) {
        this.candidateNetworks = candidateNetworks;
        this.maxTuples = maxTuples;
        this.keywords = keywords;
        this.andSemantics = andSemantics;
        this.printResultsOrderedByTable = printResultsOrderedByTable;
    }

    public ExecutionEngine(JoiningNetworkOfTupleSets network, Integer maxTuples,
                List<String> keywords, boolean andSemantics, boolean printResultsOrderedByTable) {
        this.candidateNetworks = new ArrayList<JoiningNetworkOfTupleSets>();
        this.candidateNetworks.add(network);
        this.maxTuples = maxTuples;
        this.keywords = keywords;
        this.andSemantics = andSemantics;
        this.printResultsOrderedByTable = printResultsOrderedByTable;
    }

    public ExecutionEngine(JoiningNetworkOfTupleSets network, Integer maxTuples,
                List<String> keywords, boolean andSemantics) {
        this.candidateNetworks = new ArrayList<JoiningNetworkOfTupleSets>();
        this.candidateNetworks.add(network);
        this.maxTuples = maxTuples;
        this.keywords = keywords;
        this.andSemantics = andSemantics;
        // this.printResultsOrderedByTable = printResultsOrderedByTable;
    }

    // Keep only the top K tuples form each OverloadedTupleList represents a Candidate Network result.
    public int keepTopKTuples(List<OverloadedTupleList> allTopKCNTuples) {
        // Each OverloadedTupleList tuples are sorted in a descending order.

        // Find size of results
        int allCNTuples = 0;
        for (OverloadedTupleList tupleList: allTopKCNTuples) {
            allCNTuples += tupleList.getTupleList().size();
        }

        // Each cell of this table indicated from what index we should cut off the OverloadedTupleList.
        int numOfTupleLists = allTopKCNTuples.size();
        int[] cutOffFromIndex = new int[allTopKCNTuples.size()];

        // Initialize array to have starting Index -> 0 for each TupleList.
        for (int i = 0; i < numOfTupleLists; i++) {
            cutOffFromIndex[i] = 0;
        }

        // Loop till we get maxTuples tuples.
        int resultTuples = 0;
        while (resultTuples < this.maxTuples && resultTuples < allCNTuples) {
            // In each loop check which TupleList has the tuple with a greatest score and keep it.
            int listWithGreaterTuple = -1;
            double greaterScore = -1;

            for (int listIndex = 0; listIndex < numOfTupleLists; listIndex++) {
                // Dont get out of bounds
                if (cutOffFromIndex[listIndex] == allTopKCNTuples.get(listIndex).getTupleList().size())
                    continue;

                OverloadedTuple tupleListsGreaterTuple =
                    allTopKCNTuples.get(listIndex).getTupleList().get(cutOffFromIndex[listIndex]);
                if (greaterScore < tupleListsGreaterTuple.getScore()) {
                    listWithGreaterTuple = listIndex;
                    greaterScore = tupleListsGreaterTuple.getScore();
                }
            }

            // For the list with the Greater Tuple increments it's cutOffFromIndex index.
            cutOffFromIndex[listWithGreaterTuple]++;
            resultTuples++;
        }

        // Remove all the unwanted tuples.
        List<OverloadedTupleList> unwantedTupleLists = new ArrayList<>();
        for (int listIndex = 0; listIndex < numOfTupleLists; listIndex++) {
            OverloadedTupleList tupleList = allTopKCNTuples.get(listIndex);
            // Dont get out of bounds
            if (cutOffFromIndex[listIndex] == tupleList.getTupleList().size())
                continue;        

            // Remove all tuples from cut of range to last tuple.
            allTopKCNTuples.get(listIndex)
                .getTupleList()
                .subList(cutOffFromIndex[listIndex], allTopKCNTuples.get(listIndex).getTupleList().size())
                .clear();

            // If a tuple List becomes empty then remove it.
            if (tupleList.isEmpty())
                unwantedTupleLists.add(tupleList);
        }

        // Remove all unwanted TupleLists.
        allTopKCNTuples.removeAll(unwantedTupleLists);

        // Return how many result Tuples are created.
        return resultTuples;
    }

    // Keep only the top K tuples form each OverloadedTupleList represents a Candidate Network result.
    // In addition, it computes the threshold for the Sparse execution algorithm
    // as the lowest score among all tuples.
    public Double keepTopKTuples(List<OverloadedTupleList> allTopKCNTuples, Double threshold) {
        // Find size of results
        int allCNTuples = 0;
        for (OverloadedTupleList tupleList: allTopKCNTuples) {
            allCNTuples += tupleList.getTupleList().size();
        }

        // Each cell of this table indicated from what index we should cut off the OverloadedTupleList.
        int numOfTupleLists = allTopKCNTuples.size();
        int[] cutOffFromIndex = new int[allTopKCNTuples.size()];

        // Initialize array to have starting Index -> 0 for each TupleList.
        for (int i = 0; i < numOfTupleLists; i++) {
            cutOffFromIndex[i] = 0;
        }

        // Loop till we get maxTuples tuples.
        int resultTuples = 0;
        while (resultTuples < this.maxTuples && resultTuples < allCNTuples) {
            // In each loop check which TupleList has the tuple with a greatest score and keep it.
            int listWithGreaterTuple = -1;
            double greaterScore = -1;

            for (int listIndex = 0; listIndex < numOfTupleLists; listIndex++) {
                // Dont get out of bounds
                if (cutOffFromIndex[listIndex] == allTopKCNTuples.get(listIndex).getTupleList().size())
                    continue;

                OverloadedTuple tupleListsGreaterTuple =
                    allTopKCNTuples.get(listIndex).getTupleList().get(cutOffFromIndex[listIndex]);
                if (greaterScore < tupleListsGreaterTuple.getScore()) {
                    listWithGreaterTuple = listIndex;
                    greaterScore = tupleListsGreaterTuple.getScore();
                }
            }

            // For the list with the Greater Tuple increments it's cutOffFromIndex index.            
            cutOffFromIndex[listWithGreaterTuple]++;
            resultTuples++;
        }        

        // Remove all the unwanted tuples.
        List<OverloadedTupleList> unwantedTupleLists = new ArrayList<>();
        for (int listIndex = 0; listIndex < numOfTupleLists; listIndex++) {
            OverloadedTupleList tupleList = allTopKCNTuples.get(listIndex);
            // Dont get out of bounds
            if (cutOffFromIndex[listIndex] == tupleList.getTupleList().size())
                continue;        

            // Remove all tuples from cut of range to last tuple.
            allTopKCNTuples.get(listIndex)
                .getTupleList()
                .subList(cutOffFromIndex[listIndex], allTopKCNTuples.get(listIndex).getTupleList().size())
                .clear();

            // If a tuple List becomes empty then remove it.
            if (tupleList.isEmpty())
                unwantedTupleLists.add(tupleList);
        }      

        // Remove all unwanted TupleLists.
        allTopKCNTuples.removeAll(unwantedTupleLists);        


        // Find the lowest score among the top tuples, which will be the new threshold.
        Double minScore = Double.MAX_VALUE;
        Double newThreshold = threshold;

        for (OverloadedTupleList tupleList : allTopKCNTuples) {
            Double listMinScore = tupleList.getMinScore();
            if (listMinScore < minScore) {
                minScore = listMinScore;
            }
        }

        // If the new minimum score is different from the previous one the threshold should be updated.
        if (minScore != newThreshold) {
            newThreshold = minScore;
            // System.out.println("\nNEW THRESHOLD: " + newThreshold + "\n");
        }

        // Return the new threshold.
        return newThreshold;
    }

    // Returns the top results up to the required number.
    public abstract List<OverloadedTuple> getResults();

    // Returns all the results
    public abstract List<OverloadedTuple> getAllResults();

    // The execution Engine's main function. Uses the Candidate Networks
    // to create a list of results. Every class that extends Execution Engine will implement this function.
    public abstract void execute();


    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {
        List<Table.Row> rows = new ArrayList<>();  // The table rows.
                        
 
        rows.add(new Table.Row( Arrays.asList(                
            "Total Results",
            Integer.toString(this.getAllResults().size())
        )));
                
        // Return the table containing the Components Info.
        return new Table(rows);        
    }

}
