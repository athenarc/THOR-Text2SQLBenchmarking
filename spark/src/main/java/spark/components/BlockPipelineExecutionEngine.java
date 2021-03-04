package spark.components;

import shared.connectivity.thor.response.Table;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import spark.SparkApplication;
import spark.components.BlockCreator;
import spark.components.CandidateNetworkExecutor;
import spark.model.OverloadedTuple;
import spark.model.OverloadedTupleList;
import spark.model.Block;
import spark.model.JoiningNetworkOfTupleSets;
import spark.model.ScoreType;
import spark.model.Signature;
import spark.model.TupleSet;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.PriorityQueue;

// Input: A query and a set of Candidate Networks.
// Output: A stream of the top-K answers to query.
//
// The block pipeline engine uses an algorithm described
// in detail at the spark paper on page 8. A simple explanation
// would be that the engine creates a combination of Strata
// from the TupleSets of each Candidate Networks. These
// combinations are named as Blocks. All Blocks of a Candidate
// Network cover the space of all the possible joins between tuples
// contained in it. The blocks are first ordered with a monotonic
// scoring function in a priority Queue. If a block is picked
// from the Queue a new score is calculated for this block and it's
// adjacent in solution space blocks tighter than the previous ones.
// If a block with a tight score is picked from the queue execute it
// against the RDBMS , get the results if any and calculate their real
// score . Continue this Block execution until we get K results or the
// queue is empty.
public class BlockPipelineExecutionEngine  {

    PriorityQueue<Block> blocksPQueue; // The blocks sorted in descending order based on an upper bound of their score.
    List<OverloadedTuple> results; // The top-k results sorted in descending order based on their score.
    Double threshold; // The score of the last tuple in the list of results.

    // A schema graph that contains the database tables and all the temporary tables (intermediate results)
    // that will be created during the execution.
    private SchemaGraph modifiedSchemaGraph;

    // The only modification applied to the initial database will be the addition of temporary tables
    // that will contain the intermediate results. At the end of the execution the temporary tables will be dropped.
    private SQLDatabase modifiedDatabase;

    private List<TupleSet> nonFreeTupleSets; // All the non-free tuple sets produced by the IREngine.
    
    protected List<JoiningNetworkOfTupleSets> candidateNetworks;
    protected Integer maxTuples; // The maximum number of results to return.
    protected List<String> keywords; // The keywords of the query.
    protected boolean andSemantics; // If true then the results must contain all keywords.
    protected boolean printResultsOrderedByTable; // If true print results ordered by the tables they belong to.

    List<OverloadedTupleList> allTopKCNTuples; // The list of results.


    // stats 
    private Integer numOfSqlQueriesExecuted; // The number of SQL queries executed by the executor.

    public BlockPipelineExecutionEngine (List<JoiningNetworkOfTupleSets> networks, SchemaGraph schemaGraph,
            SQLDatabase database, Integer maxTuples, List<String> keywords, boolean efficientPlanGenerator,
            List<TupleSet> nonFreeTupleSets, boolean andSemantics, boolean printResultsOrderedByTable) {
        this.candidateNetworks = networks;
        this.maxTuples = maxTuples;
        this.keywords = keywords;
        this.andSemantics = andSemantics;
        this.printResultsOrderedByTable = printResultsOrderedByTable;
        this.results = new ArrayList<OverloadedTuple>();
        this.allTopKCNTuples = new ArrayList<>(); // The list of results
        this.threshold = 0.0;
        this.blocksPQueue = new PriorityQueue<>(new Block.ScoreComparator());
        this.modifiedSchemaGraph = schemaGraph;
        this.modifiedDatabase = database;
        this.nonFreeTupleSets = nonFreeTupleSets;
        this.numOfSqlQueriesExecuted = 0;
        Signature.setKeywords(this.keywords); // Set the static keywords array of the Signature class.
    }

    // Getters and Setters.
    public List<OverloadedTuple> getResults() {
        if (this.results.size() > this.maxTuples) {
            return this.results.subList(0, this.maxTuples);
        }
        else {
            return this.results;
        }
    }

    public List<OverloadedTuple> getAllResults() {        
        return this.results;
    }
    
    public List<OverloadedTupleList> getAllTopKCNTuples() {
        return allTopKCNTuples;
    }

    /**
     * @return the numOfSqlQueriesExecuted
     */
    public Integer getNumOfSqlQueriesExecuted() {
        return numOfSqlQueriesExecuted;
    }
    

    // Initialize the queue of blocks by adding the first block of each candidate network.
    public void initializeBlocksQueue() {
        for (JoiningNetworkOfTupleSets network: this.candidateNetworks) {
            // Compute the network's constant values needed for the scoring formulas.
            network.computeConstantValues(this.keywords);

            // Initialize the block creator which is responsible for generating the blocks of the network.
            // During the initialization process the non-free tuple are split into strata.
            BlockCreator blockCreator = new BlockCreator(network);

            // Create the first block of the current network.
            Block firstBlock = blockCreator.createFirstBlock();

            // System.out.println("Size Normalization Factor = " + network.getSizeNormalizationFactor());
            // System.out.println("Keyword idfs = " + network.getKeywordIdfs());

            // Calculate the USCORE (upper bound of the real score) of the block.
            firstBlock.setStatus(ScoreType.USCORE);
            firstBlock.setScore(Double.valueOf(firstBlock.computeUScore()));

            // Debug prints
            if (SparkApplication.DEBUG_PRINTS) {
                System.out.println("Creating the first block for the network: " + network.toAbbreviation() + "\n");
                System.out.println("\n- First Block Created -");
                System.out.println(firstBlock + "\n");
                System.out.println("Block uscore = " + firstBlock.getScore() + "\n");
                System.out.println("----------------------------------\n");
            }

            // Push the block into the priority queue.
            this.blocksPQueue.add(firstBlock);
        }

        if (SparkApplication.DEBUG_PRINTS)
            System.out.println("===================================\n");
    }

    // Returns true if the execution must stop.
    public boolean finalizingCondition() {
        // If there are no more blocks to examine, stop the execution.
        if (this.blocksPQueue.isEmpty()) { return true; }

        // If the desired number of results has been produced, stop the execution.
        if (this.results.size() >= this.maxTuples) { return true; }

        // If there is a block in the priority queue with an upper bound higher than
        // the actual score of the result with the smallest score, then continue the execution,
        // since that block may produce better results.
        if (this.threshold < this.blocksPQueue.peek().getScore()) { return false; }

        return true;
    }

    // Updates the threshold.
    public void updateThreshold() {
        if (this.results.size() < maxTuples) {
            this.threshold = -1.0;
        }
        else {
            this.threshold = this.results.get(maxTuples-1).getScore();
        }
    }

    // This function contains the main functionality of the execution engine component.
    public void execute() {
        // Create a Candidate Network Executor and initialize it.
        CandidateNetworkExecutor candidateNetworkExecutor = new CandidateNetworkExecutor(
            this.modifiedSchemaGraph, this.modifiedDatabase, this.nonFreeTupleSets, this.maxTuples
        );        

        // Initialize the priority queue with the first block of each candidate network. (Lines 2-5 from Algorithm 2)
        initializeBlocksQueue();

        // Loop until the finalizing condition is satisfied.
        while (this.finalizingCondition() == false) {
            Block head = this.blocksPQueue.remove();

            if (head.getStatus() == ScoreType.USCORE) {
                // Push the block back into the queue with its bscore value.
                head.setStatus(ScoreType.BSCORE);
                head.setScore(Double.valueOf(head.computeBScore()));
                this.blocksPQueue.add(head);

                if (SparkApplication.DEBUG_PRINTS)
                    System.out.println("Block new bscore = " + head.getScore() + "\n");

                // Create the adjacent blocks and push them into the queue with their uscore value.
                for (Block adjacent : head.getBlockCreator().createAdjacentBlocks(head)) {
                    this.blocksPQueue.add(adjacent);
                }
            }
            else if (head.getStatus() == ScoreType.BSCORE) {
                if (SparkApplication.DEBUG_PRINTS)
                    System.out.println("Execute:\n\tnetwork: " + head.toAbbreviation() + " \n\tblock: " + head.getSignature() + "\n");

                // Execute the block tree.
                OverloadedTupleList resultTuples = candidateNetworkExecutor.execute(head);
                this.numOfSqlQueriesExecuted++;
                if (resultTuples.isEmpty()) continue;

                // Truncate Results if and Semantics: ADDED NOW
                if (this.andSemantics)
                    resultTuples.truncate(this.keywords);
                
                // Add every result to the list of top tuples.
                for (OverloadedTuple result : resultTuples.getTupleList()) {                    
                    result.setScore(Double.valueOf(result.computeScore(head)));
                    // result.setScore(head.getScore());
                    this.results.add(result);
                }

                if (SparkApplication.DEBUG_PRINTS) {
                    System.out.println("RESULTS:\n"); 
                    resultTuples.print(true);
                }

                this.allTopKCNTuples.add(resultTuples);

                // Sort the list of results and update the threshold.
                Collections.sort(this.results, new OverloadedTuple.ScoreComparator());
                this.updateThreshold();
            }
        }

        // // If andSemantics boolean is true, truncate the results 
        // // tuples to only those containing all keywords.
        // if (this.andSemantics) {
        //     for (OverloadedTupleList tupleList: this.allTopKCNTuples) {                
        //         tupleList.truncate(this.keywords);                
        //     }

        //     // Remove the empty SQTupleLists
        //     List<OverloadedTupleList> tuplesListToRemove = new ArrayList<>();
        //     for (OverloadedTupleList tupleList: this.allTopKCNTuples) {
        //         if (tupleList.isEmpty())
        //             tuplesListToRemove.add(tupleList);
        //     }
        //     this.allTopKCNTuples.removeAll(tuplesListToRemove);

        //     // Remove tuples from the results too
        //     this.results = new ArrayList<>();
        //     for(OverloadedTupleList tupleList: allTopKCNTuples) {
        //         this.results.addAll(tupleList.getTupleList());
        //     }

        //     // Sort the list.
        //     Collections.<OverloadedTuple>sort(this.results, new OverloadedTuple.ScoreComparator());
        // }   
    }

    // Prints the result tuples.
    public void printResults() {
        // Print the Engine's Name.
        System.out.println("BlockPipeline Execution Engine's Top " + this.maxTuples + " results\n");        

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
                System.out.println(tuple.toString());
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


    /**
     * Prints a string incase Debug Prints is true
     * @param x
     */
    static void debugPrintln(String x) {
        if  (SparkApplication.DEBUG_PRINTS)
            System.out.println(x);
    }

    static void debugPrintln() {
        if  (SparkApplication.DEBUG_PRINTS)
            System.out.println();
    }

}
