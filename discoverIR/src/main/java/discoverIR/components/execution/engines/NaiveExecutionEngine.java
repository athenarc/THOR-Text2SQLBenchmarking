package discoverIR.components.execution.engines;

import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import discoverIR.components.execution.executors.CandidateNetworkExecutor;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.OverloadedTuple;
import discoverIR.model.OverloadedTupleList;
import discoverIR.model.TupleSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NaiveExecutionEngine extends ExecutionEngine {

    // A Schema Graph Containing the database Tables and all the Temp
    // Tables (representing Intermediate Results) that will be created
    // while executing this Plan.
    private SchemaGraph modifiedSchemaGraph;

    // The only modification applied in the Database created in the begging of
    // the program will be the addition of Temp Tables containing then
    // the intermediate results.
    // At the end of the Executor all the Temp Tables will be dropped.
    private SQLDatabase modifiedDatabase;

    private List<TupleSet> nonFreeTupleSets; // All the non-free tuple sets produced by the IREngine.
    private List<OverloadedTuple> results; // List to save the results.
    private List<OverloadedTupleList> allTopKCNTuples; // The list of results grouped by network.

    public NaiveExecutionEngine(List<JoiningNetworkOfTupleSets> candidateNetworks, SchemaGraph schemaGraph,
            SQLDatabase database, List<TupleSet> nonFreeTupleSets, Integer maxTuples, List<String> keywords,
            boolean andSemantics, boolean printResultsOrderedByTable, boolean efficientPlanGenerator) {
        super(candidateNetworks, maxTuples, keywords, andSemantics, printResultsOrderedByTable);
        this.modifiedSchemaGraph = new SchemaGraph(schemaGraph);
        this.modifiedDatabase = database;
        this.nonFreeTupleSets = nonFreeTupleSets;
        this.results = new ArrayList<OverloadedTuple>();
        this.allTopKCNTuples = new ArrayList<>();
    }

    // Returns the top results up to the required number.
    @Override
    public List<OverloadedTuple> getResults() {
        if (this.results.size() > super.maxTuples) {
            return this.results.subList(0, super.maxTuples);
        } else {
            return this.results;
        }
    }

    public List<OverloadedTuple> getAllResults() {
        return this.results;
    }

    public List<OverloadedTupleList> getAllTopKCNTuples() {
        return allTopKCNTuples;
    }

    // Executes the candidate networks and saves the results.
    @Override
    public void execute() {
        // Initialize a Candidate Network Executor.
        CandidateNetworkExecutor executor = new CandidateNetworkExecutor(modifiedSchemaGraph, modifiedDatabase,
                nonFreeTupleSets, super.maxTuples);
        executor.initializeExecutor();

        // Loop through the candidate networks and execute them.
        for (JoiningNetworkOfTupleSets candidateNetwork: super.candidateNetworks) {
            allTopKCNTuples.add(executor.execute(candidateNetwork));
        }
        
        // Restore the changes made by the executor.
        executor.finalizeExecutor();

        // If the andSemantics boolean is true, truncate the results to only those containing all keywords.
        if (this.andSemantics) {
            for (OverloadedTupleList tupleList: allTopKCNTuples) {       
                tupleList.truncate(this.keywords);
            }

            // Remove the empty SQTupleLists.
            List<OverloadedTupleList> tuplesListToRemove = new ArrayList<>();
            for (OverloadedTupleList tupleList : allTopKCNTuples) {
                if (tupleList.isEmpty()) {
                    tuplesListToRemove.add(tupleList);
                }
            }

            allTopKCNTuples.removeAll(tuplesListToRemove);
        }

        // Create a single list containing all top tuples.
        for (OverloadedTupleList tupleList: allTopKCNTuples) {
            this.results.addAll(tupleList.getTupleList());
        }

        // Sort the list.
        Collections.sort(this.results, new OverloadedTuple.ScoreComparator());

        this.printResults(allTopKCNTuples);
    }

    // Prints the result tuples.
    private void printResults(List<OverloadedTupleList> allTopKCNTuples) {
        // Print the Engine's Name.
        System.out.println("Naive Execution Engine's Top " + this.maxTuples + " results\n");        

        // The number of tuples
        int resultTuplesNumber = 0;

        // Depending on the printResultsOrderedByTable boolean value we will print
        //  the tuples ordered by the table they belong to or as single tuples.
        if (this.printResultsOrderedByTable) {
            // Keep only the top K tuples from all CN Tuples.
            resultTuplesNumber = this.keepTopKTuples(allTopKCNTuples);

            // Print the results            
            for (OverloadedTupleList tupleList: allTopKCNTuples)  {
                tupleList.print(true);
                System.out.println();
            }            
        }
        else {
            // Create a list containing all tuples.
            List<OverloadedTuple> resultTuples = new ArrayList<>();
            for(OverloadedTupleList tupleList: allTopKCNTuples) {
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

            // Update the resultTuples number
            resultTuplesNumber = resultTuples.size();
        }


        // Finishing prints
        if (resultTuplesNumber == 0)
            System.out.println("(Empty Set)");        
        else if (resultTuplesNumber < this.maxTuples)
            System.out.println("(No more than " + resultTuplesNumber + " result tuples were accepted)");        
    }

}
