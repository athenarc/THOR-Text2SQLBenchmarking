package discover.components.executors;

import discover.model.execution.Assignment;
import discover.model.execution.CandidateNetworkAssignment;
import discover.model.execution.ExecutionPlan;
import discover.model.execution.IntermediateResultAssignment;
import discover.model.TupleSet;
import discover.DiscoverApplication;
import discover.components.ExecutionPreProcessor;
import discover.model.OverloadedTuple;
import discover.model.OverloadedTupleList;
import shared.database.model.graph.SchemaGraph;
import shared.connectivity.thor.response.Table;
import shared.database.model.SQLDatabase;
import shared.util.PrintingUtils;
import shared.util.Timer;
import shared.util.Timer.Type;

import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

// Input: An execution Plan, a schemaGraph and a database.
// Output: The results of the candidate networks are printed
//         in the standard output of the program.
// The PlanExecutor class takes an execution plan, and executes each
// assignment it contains. There are two types of assignments, the 
// one type creates Intermediate Results and the other uses the 
// Intermediate Results to create the Candidate Network's tuples.
// In the process of the Plan Execution Temporary Tables are 
// created storing the Intermediate Results. A copy of the 
// schema graph is updated and also the database instance.
public class PlanExecutor {

    List<OverloadedTuple> results; // The results of the execution.
    List<OverloadedTupleList> orderedResults;

    private SchemaGraph schemaGraph; // The schemaGraph.

    // The only modification applied in the Database created in the begging of
    // the program will be the addition of Temp Tables containing then
    // the intermediate results.
    // At the end of the Executor all the Temp Tables will be dropped.
    private SQLDatabase modifiedDatabase;

    // The preProcessor is called before the Execution of the assignments 
    // to create tempTable for all the tupleSets used in the Assignments.
    // This optional process may speed up the joins because the tables used 
    // in the joins will be smaller than the original Tables of the database.
    ExecutionPreProcessor preProcessor;

    int maxTuples; // The maximum number of tuples to be returned to the user.

    // Statistics.
    private double timeExecutingPlan;

    public PlanExecutor(SchemaGraph schemaGraph, SQLDatabase database, int maxTuples) {
        this.results = new ArrayList<OverloadedTuple>();
        this.orderedResults = new ArrayList<>();
        this.modifiedDatabase = database;
        this.schemaGraph = schemaGraph;
        this.maxTuples = maxTuples;
        this.preProcessor = null;
    }

    // Returns the top results up to the required number.
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

    /**
     * @return the orderedResults
     */
    public List<OverloadedTupleList> getOrderedResults() {
        return orderedResults;
    }


    // Executes the generated plan and saves the results.
    public void execute(ExecutionPlan executionPlan, List<TupleSet> tupleSets) {
        Timer timer = new Timer(Type.WALL_CLOCK_TIME);
        timer.start();

        // Initialize the execution pre processor.
        preProcessor = new ExecutionPreProcessor(tupleSets, this.modifiedDatabase);
        
        // System.out.println("EXECUTORS PRE PROCESSOR");
        // preProcessor.createTempTablesForTupleSets();
        // System.out.println("-----------------\n");

        // Convert each assignment to an SQL query.
        for (Assignment assignment : executionPlan.getAssignments()) {

            // Debug prints
            if (DiscoverApplication.DEBUG_PRINTS)
                System.out.println("Executing assignment " + assignment.toAbbreviation() + "\n");
            
            // Execute the assignment.
            try {
                Timer assignmentTimer = new Timer(Type.WALL_CLOCK_TIME);
                assignmentTimer.start();

                // Execute the Assignment with the appropriate AssignmentExecutor depending on its instance type.
                if (assignment instanceof IntermediateResultAssignment) {
                    // This assignment does not return anything, only modifies the database.
                    IntermediateResultAssignmentExecutor.execute((IntermediateResultAssignment) assignment,
                        this.preProcessor, this.schemaGraph, this.modifiedDatabase
                    );
                }
                else if (assignment instanceof CandidateNetworkAssignment) {
                    // This assignment returns the results tuples of the execution.
                    List<OverloadedTuple> rs = CandidateNetworkAssignmentExecutor.execute((CandidateNetworkAssignment) assignment,
                        this.schemaGraph, this.modifiedDatabase
                    );
                    
                    if (!rs.isEmpty()) {
                        OverloadedTupleList otl = new OverloadedTupleList(rs);
                        otl.setNetwork(assignment.toString());
                        this.orderedResults.add(otl);
                        this.results.addAll(rs);
                    }
                }

                assignment.executionTime = assignmentTimer.stop();
            } 
            catch (Exception e) {
                e.printStackTrace();
                break;
            }

            // Debug prints
            if (DiscoverApplication.DEBUG_PRINTS)
                System.out.println("---------" + "\n");
        }   
        
        // Restore the changes done by the pre processor to the database.
        // preProcessor.restoreChanges();
        preProcessor.dropAllTempTables();
        this.timeExecutingPlan = timer.stop();
    }

    // Print the statistics. Optional take an execution plan and print
    // the time it took for each assignment to execute.
    public void printStats(ExecutionPlan executionPlan) {
        System.out.println("PLAN EXECUTOR STATS :");
        System.out.println("\tTime to execute the Plan: " + this.timeExecutingPlan);
        this.preProcessor.printStats();

        // Print the execution plan stats.
        if (executionPlan != null);

        // Loop once to find the longest assignment.toString();
        Integer maxAssignmentChars = 0;
        for (Assignment assignment: executionPlan.getAssignments()) {
            Integer assignmentChars = assignment.toString().length();
            // Store the largest assignment in Chars.
            if (assignmentChars > maxAssignmentChars) maxAssignmentChars = assignmentChars;
        }
        
        System.out.println("Time it took to execute Each assignment:");
        for (Assignment assignment: executionPlan.getAssignments()) {
            System.out.println(
                "\t" + PrintingUtils.addStringWithLeadingChars(
                    maxAssignmentChars + 2, assignment.toString(), " ") +
                "{" + assignment.executionTime + " (s)}"
            );
        }
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

}