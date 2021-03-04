package discover.model.execution;

import java.util.List;
import java.util.ArrayList;

// Given a set of {C1, ..., Cr} candidate networks, an execution plan
// is a list {A1, ..., As} of assignments of the form Hi <-- Bi1 |><| ... |><| Bit,
// where Bij is either a tuple set or an intermediate result,
// and for each candidate network C there is an assignment Ai that computes C.
public class ExecutionPlan {

    private List<Assignment> assignments; // The assignments in the plan.

    public ExecutionPlan() {
        this.assignments = new ArrayList<Assignment>();
    }

    // Returns the assignment list.
    public List<Assignment> getAssignments() {
        return assignments;
    }

    // Adds an assignment.
    public void addAssignment(Assignment assignment) {
        this.assignments.add(assignment);
    }

    // Removes a list of Assignments.
    public void removeAllAssignments(List<Assignment> assignmentsToRemove) {
        this.assignments.removeAll(assignmentsToRemove);
    }

    // Returns the assignments where the parameter assignmentTerm is used in this executionPlan.
    // The startIndex indicates the index of the intermediateResultAssignment. Every assignment
    // using it must be in an index above startIndex.
    public List<Assignment> getAssignmentsUsingTerm(
        IntermediateResultAssignment intermediateResultTerm,
        int startIndex)
    {
        List<Assignment> assignmentsList = new ArrayList<>();

        // Loop the assignments and find out what assignments use that assignmentTerm.
        for (int index = startIndex; index < this.assignments.size(); index++) {
            Assignment assignment = this.assignments.get(index);
            if (assignment.containsIntermediateResult(intermediateResultTerm))
                assignmentsList.add(assignment);
        }
        
        // Return the assignment List.
        return assignmentsList;
    }

    @Override
    public String toString() {
        String str = new String();
        for (Assignment assignment: this.assignments) {
            str += assignment + "\n";
        }
        return str;
    }
}
