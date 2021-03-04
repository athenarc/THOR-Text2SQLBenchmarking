package discover.model.execution;

import discover.model.JoinableExpression;

// An assignment stores a list of joining tuple sets or intermediate results
// adhering the form Hi <-- Bi1 |><| .... |><| Bit.
//
// An intermediate result is an assignment that was previously generated in the execution plan.
// As the paper indicates the assignments Hi will only contain lists of size two,
// as a result we can use a JoinableExpression to model the assignment Terms. (see JoinableExpression)
public abstract class Assignment {

    // The id+prefix combination is unique for every assignment.
    private int id; // The assignment id.
    private String prefix; // It can be "T" for temporary or "C" for candidate network.
    private JoinableExpression assignmentTerms; // The terms of the assignment stored in a JoinableExpression.

    // Statistics 
    public double executionTime;

    public Assignment(int id, String prefix, JoinableExpression terms) {
        this.id = id;
        this.prefix = prefix;
        this.assignmentTerms = terms;
    }

    // Getters and setters.
    public JoinableExpression getAssignmentTerms() {
        return assignmentTerms;
    }            

    // Returns true if it contains the parameter iResultAssignment
    public boolean containsIntermediateResult(IntermediateResultAssignment intermediateResult) {       
        return this.assignmentTerms.containsIntermediateResult(intermediateResult);
    }

    // Replaces an assignmentTerm with its contains, if it is part of this assignment.
    // The assignment Term need to be an IntermediateResult assignment.
    // Returns false if the assignmentTerm is not part of this assignment and 
    // true if the replacement process was successful.
    public boolean replaceIntermediateResultWithContents(IntermediateResultAssignment intermediateResult) {
        return this.assignmentTerms.removeIntermediateResultAssignment(intermediateResult);
    }  

    // Returns an abbreviation of the assignment.
    public String toAbbreviation() {
        return this.prefix + this.id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof Assignment)) return false;

        // Each assignment's id+prefix is unique.
        Assignment assignment = (Assignment) obj;
        String thisAssignmentName = this.prefix + this.id;
        String objAssignmentName = assignment.prefix + assignment.id;

        return thisAssignmentName.equals(objAssignmentName);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + this.id;
        hash = 31 * hash + ((this.prefix != null) ? this.prefix.hashCode() : 0);
        return hash;
    }

    @Override
    public String toString() {                
        return this.prefix + this.id + " <== [" + this.assignmentTerms.toAbbreviation() + "]";
    }

}
