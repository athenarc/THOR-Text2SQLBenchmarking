package discover.components;

import discover.model.execution.Assignment;
import discover.model.execution.CandidateNetworkAssignment;
import discover.model.execution.ExecutionPlan;
import discover.model.execution.IntermediateResultAssignment;
import shared.connectivity.thor.response.Table;
import shared.util.Timer;
import discover.model.JoiningNetworkOfTupleSets;
import discover.model.TupleSet;
import discover.model.JoinableExpression;
import discover.model.JoinablePair;

import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;

// Input: A set S of candidate networks {C1, ..., Cr}.
// Output: An execution plan to evaluate them.
//
// They key optimization here is that the CNs share common join sub-expressions.
// The efficient plan that this component produces stores this common join
// sub-expressions as intermediate results and reuses them when needed.
public class PlanGenerator {

    // Statistics
    private double timeGeneratingPlan;
    private double timeVictimizingAssignments;
    private Integer cnAssignments = 0;
    private Integer irAssignments = 0;

    // Public constructor.
    public PlanGenerator() {}

    // Generates an execution plan.
    // Selects common join sub-expressions of 1 join inside 
    // the candidate networks that are frequently used.
    public ExecutionPlan generateExecutionPlan(List<JoiningNetworkOfTupleSets> candidateNetworks) {
        Timer timer = new Timer();
        timer.start(); // Start the timer.
        ExecutionPlan executionPlan = new ExecutionPlan(); // The execution Plan.
        
        // Keeps track of the candidate networks inserted in the plan.
        BitSet candidateNetworksInExecutionPlan = new BitSet(candidateNetworks.size());

        // Handle the Special case where a Candidate Network contains
        // only one TupleSet (the root), and has no Joins.
        this.createAssignmentsForCNsContainingNoJoins(
            candidateNetworksInExecutionPlan,
            candidateNetworks, executionPlan
        );        

        // Loop until every candidate network is in the execution plan.
        while (candidateNetworksInExecutionPlan.cardinality() != candidateNetworks.size()) {        
            // Indicates whether the assignment created in this loop
            // is the last assignment for creating a candidate network.
            boolean assignmentCreatesCN = false;
            
            // Maps the subExpressions with their number of occurrences in the candidate networks.
            SubExpressionOccurrencesMap subExprOccurrencesMap = getPairOccurrencesMap(
                candidateNetworks, candidateNetworksInExecutionPlan
            );

            // Get the subExpression with the largest frequency() value.
            JoinablePair bestSubExpression = subExprOccurrencesMap.peek();
            // System.out.println("Adding to IR: " + bestSubExpression + "\n");            

            // Rewrite all candidate networks to use the best joinable pair when possible.            
            assignmentCreatesCN = this.rewriteCandidateNetworks(
                candidateNetworks,
                candidateNetworksInExecutionPlan,
                bestSubExpression
            );            

            // Every loop creates an assignment for the Execution Plan,
            // weather it creates a candidate network , or simply an Intermediate Result
            Assignment assignment = null;
            if (assignmentCreatesCN) {
                assignment = new CandidateNetworkAssignment(bestSubExpression);
                cnAssignments++;
            }
            else {
                assignment = new IntermediateResultAssignment(bestSubExpression);
                irAssignments++;
                // Update the subExpression with the intermediate result assignment.
                bestSubExpression.setAssignmentCreatingIntermediateResult(
                    (IntermediateResultAssignment) assignment
                );
            }

            // Add assignment it in the execution plan.
            executionPlan.addAssignment(assignment);

            // System.out.println("-------");
        }

        this.timeGeneratingPlan = timer.stop();
        // System.out.println("ExecutionPlan before victimization:\n" + executionPlan);

        // Call the PostProcessing Function.
        this.victimizeIntermediateResults(executionPlan);
        // System.out.println("ExecutionPlan after victimization:\n" + executionPlan);


        // Return the Execution Plan.
        return executionPlan;
    }


    // The PlanGenerators Post Processing function. Traces Intermediate Result that are 
    // used only once in the Execution Plan and replaces them with their contents.
    // This saves us from the extra Computation Time of creating Intermediate Results 
    // that are barely used.
    private void victimizeIntermediateResults(ExecutionPlan executionPlan) {
        Timer timer = new Timer();
        timer.start(); // Start the timer.

        // Get the Assignment List from the executionPlan
        List<Assignment> assignmentList = executionPlan.getAssignments();
        List<Assignment> intermediateResultAssignmentsToRemove = new ArrayList<>();

        // Loop each Intermediate Result Assignment and find its occurrences in the Execution Plan.        
        for (int index = 0; index < assignmentList.size(); index++) {
            Assignment assignment = assignmentList.get(index);

            // Process only the Intermediate Result Assignments.
            if (!(assignment instanceof IntermediateResultAssignment)) continue;
            IntermediateResultAssignment intermediateResult = (IntermediateResultAssignment) assignment;

            // Vitimize Intermediate results that :
            // 1) Are not used more than once in the execution plan
            // 2) Are complied by free tuple Sets (too much computations for no reason)
            
            // Get all the Assignments using this intermediateResult after this assignments index.
            List<Assignment> assignmentsUsingIRres = executionPlan.getAssignmentsUsingTerm(intermediateResult, index + 1);
            // Get the expression creating this intermediate result.
            JoinableExpression expression = intermediateResult.getAssignmentTerms();   

            // (1) If the intermediateResultTerm is used only in one Assignment replace the term 
            // with its contains and remove this intermediate Result assignment from the executionPlan.
            if (assignmentsUsingIRres.size() == 1) {
                Assignment assignmentToReform = assignmentsUsingIRres.get(0);
                assignmentToReform.replaceIntermediateResultWithContents(intermediateResult);   

                // Remove the assignment
                intermediateResultAssignmentsToRemove.add(assignment);
            }
            // (2) If the intermediate result is complied by free tuple sets then remove it because it is not 
            // useful at all and adds computation to the Systems execution.
            else if (expression instanceof JoinablePair && ((JoinablePair) expression).isCompliedByFreeTupleSets()) {
                // Remove this intermediate result from all assignments using it.
                for (Assignment assignmentToReform: assignmentsUsingIRres) {
                    assignmentToReform.replaceIntermediateResultWithContents(intermediateResult);
                }

                // Remove the assignment                
                intermediateResultAssignmentsToRemove.add(assignment);
            }                        
        }

        // Remove the unwanted assignments.
        executionPlan.removeAllAssignments(intermediateResultAssignmentsToRemove);
        this.timeVictimizingAssignments = timer.stop(); // Stop the timer.
    }




    // Handle the Special case where a Candidate Network contains only one TupleSet (the root), 
    // and has no Joins. In this case the PlanGenerator creates an assignment Containing only 
    // one assignment Term. This term is the TupleSet of the CandidateNetworks root.
    private void createAssignmentsForCNsContainingNoJoins(
        BitSet candidateNetworksInExecutionPlan,
        List<JoiningNetworkOfTupleSets> candidateNetworks, 
        ExecutionPlan executionPlan)
    {
        // Loop all the candidate networks.
        for (int index = 0; index < candidateNetworks.size(); index++) {         

            // Get the Candidate Network
            JoiningNetworkOfTupleSets jnts = candidateNetworks.get(index);
                
            // If no joins are contained in the Candidate Network.
            if (jnts.getSize() == 0) {                
                // Create an assignment with a single TupleSet.
                TupleSet tupleSet = jnts.getRootsTupleSet();
                Assignment assignment = new CandidateNetworkAssignment(tupleSet);                
                executionPlan.addAssignment(assignment);
                cnAssignments++;

                // The CN is in the execution plan now.
                candidateNetworksInExecutionPlan.set(index);
            }
        }
    }


    
    // Returns a map all the sub-expressions of 1 join and their occurrences in all the candidate networks.
    private SubExpressionOccurrencesMap getPairOccurrencesMap(
        List<JoiningNetworkOfTupleSets> candidateNetworks,
        BitSet candidateNetworksInExecutionPlan) 
    {
        // Maps the pairs with their number of occurrences in the candidate networks.
        SubExpressionOccurrencesMap subExprOccurrencesMap = new SubExpressionOccurrencesMap();

        // System.out.println("PAIRS\n");

        // Get all tuple set pairs from the candidate networks and add them to the hashmap.
        for (int index = 0; index < candidateNetworks.size(); index++) {
            // If the CN is already it the execution plan then skip it
            if (candidateNetworksInExecutionPlan.get(index)) continue;

            // Get the Candidate Network
            JoiningNetworkOfTupleSets jnts = candidateNetworks.get(index);            
            for (JoinablePair pair : jnts.getAdjacentJoinablePairs()) {                    
                // System.out.println(pair);
               subExprOccurrencesMap.put(pair, subExprOccurrencesMap.getOrDefault(pair, 0) + 1);
            }
       }
        // System.out.println();            
        // System.out.println("Pair Map: " + subExprOccurrencesMap);

       return subExprOccurrencesMap;
    }


    // Rewrites all candidate networks to use the best sub Expression when possible.
    // Also returns a boolean showing if this sub Expression is responsible for creating a 
    // Candidate network.
    private boolean rewriteCandidateNetworks(
        List<JoiningNetworkOfTupleSets> candidateNetworks,
        BitSet candidateNetworksInExecutionPlan,
        JoinablePair bestSubExpression)
    {
        // Indicates whether the bestSubExpression is responsible for creating a Candidate network.
        boolean createsCandidateNetwork = false;

        // Rewrite all candidate networks to use the best sub Expression when possible.
        // System.out.println("Re-write Candidate Networks:");
        for (int index = 0; index < candidateNetworks.size(); index++) {

            // If the CN is already it the execution plan then skip it
            if (candidateNetworksInExecutionPlan.get(index)) continue;            

            // Get the Candidate Network and rewrite it.
            JoiningNetworkOfTupleSets jnts = candidateNetworks.get(index);                                
            jnts.rewriteJntsUsingIntermediateResult(bestSubExpression);

            // If Candidate Network's size is 0 after rewriting it
            // then the tree is created by the execution Plan
            if (jnts.getSize() == 0) {
                candidateNetworksInExecutionPlan.set(index);
                createsCandidateNetwork = true; // This IR is the last assignment of the CN of index @index.

            }           
            // System.out.println("\n" + jnts + "\n");
        }

        return createsCandidateNetwork;
    }


    // Generates an execution plan.
    // Selects common join sub-expressions of 1 join inside 
    // the candidate networks that are frequently used.
    public ExecutionPlan generateExecutionPlan_CN_ONLY(List<JoiningNetworkOfTupleSets> candidateNetworks) {
        Timer timer = new Timer();
        timer.start(); // Start the timer.
        ExecutionPlan executionPlan = new ExecutionPlan(); // The execution Plan.

        for (JoiningNetworkOfTupleSets jnts : candidateNetworks) {
            executionPlan.addAssignment(new CandidateNetworkAssignment(jnts));
        }

        this.timeGeneratingPlan = timer.stop();
        // System.out.println("ExecutionPlan before victimization:\n" + executionPlan);

        // Return the Execution Plan.
        return executionPlan;
    }


    // Print the statistics
    public void printStats() {
        System.out.println("PLAN GENERATOR STATS :");
        System.out.println("\tTime to generate Plan: " + this.timeGeneratingPlan);
        System.out.println("\tTime to victimize Assignments: " + this.timeVictimizingAssignments);
    }

    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {
        List<Table.Row> rows = new ArrayList<>();  // The table rows.
       
        rows.addAll( Arrays.asList(            
            new Table.Row( Arrays.asList(                
                "Assignments creating intermediate tables",
                Integer.toString(this.irAssignments)
            )),
            new Table.Row( Arrays.asList(                
                "Assignments creating final results",
                Integer.toString(this.cnAssignments)
            ))
        ));
                
        // Return the table containing the Components Info.
        return new Table("Type of assignments", rows);
    }

}



// A Class storing unique JoinablePairs ordered by their occurrences 
// as sub expression in the Candidate Network. To achieve this we keep
// a HashMap and a Priority queue both storing references to the pairs.
class SubExpressionOccurrencesMap {
    // The HashMap
    Map<JoinablePair, Integer> pairMap; 

    // The priority queue ordering JoinablePairs by 
    // their occurrences in the Candidate Networks.
    PriorityQueue<OverloadedJoinablePair> pairPQueue; 

    // Constructor.
    SubExpressionOccurrencesMap() {
        this.pairMap = new HashMap<>();
        this.pairPQueue = new PriorityQueue<>(new OverloadedJoinablePair.OccurrencesComparator());
    }

    // Put a joinable pair with its occurrences in the map.
    void put (JoinablePair pair, Integer occurrences) {
        OverloadedJoinablePair overloadedPair = new OverloadedJoinablePair(pair, occurrences);
        // Add the pair to both data structures.
        Integer oldOccurrences = this.pairMap.put(pair, occurrences);
        if (oldOccurrences != null) {
            this.pairPQueue.remove(overloadedPair); // Remove the pair to re-add it with the new Integer.
        }
        this.pairPQueue.add(overloadedPair);  // And the pair to the pq.
    }

    // Return true if the Map is empty.
    boolean isEmpty() { 
        return this.pairMap.isEmpty(); 
    }

    // Returns the pair with the greater occurrence.
    JoinablePair peek() {
        if (this.isEmpty()) return null;

        // Get the top pair from the Priority Queue 
        OverloadedJoinablePair topPair = this.pairPQueue.poll();
        return topPair.joinablePair;
    }

    // Returns the Occurrences of a Joinable pair or default if it does not exist.
    Integer getOrDefault(JoinablePair pair, Integer def) {
        return this.pairMap.getOrDefault(pair, def);
    }

    @Override
    public String toString() {
        return this.pairMap.toString();
    }

}

// This class keeps a Joinable pair and an integer indicating the
// pairs occurrences in the Candidate networks as a sub-expression.
class OverloadedJoinablePair {

    // The comparator for the Priority Queue
    public static class OccurrencesComparator implements Comparator<OverloadedJoinablePair> {
        @Override
        public int compare(OverloadedJoinablePair a, OverloadedJoinablePair b) {
            return b.occurrences.compareTo(a.occurrences);
        }
    }

    JoinablePair joinablePair;  // The joinable pair.
    Integer occurrences;  // The occurrences.

    // Constructor.
    OverloadedJoinablePair(JoinablePair joinablePair, Integer occurrences) {
        this.joinablePair = joinablePair;
        this.occurrences = occurrences;
    }

    @Override
    public int hashCode() {
        return joinablePair.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return joinablePair.equals(obj);
    }

    @Override
    public String toString() {
        return this.joinablePair.toString() + " : " + this.occurrences;
    }
}