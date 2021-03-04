package discover.model;

import java.util.HashSet;
import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import discover.model.execution.IntermediateResultAssignment;


// Contains a pair of joinable expressions (because the paper only considers joins of two expressions).
// The expressions that it contains can be a combination of tuple sets or an intermediate result.
// The JoinablePair models an intermediate Result.
// Also the joinable pair models the sub expressions of 1 join inside an Candidate Network.
public class JoinablePair implements JoinableExpression {

    private JoinableExpression left;
    private JoinableExpression right;
    private Set<SQLColumn> columnsContainingKeywords;
    private SQLTable table; // The SQL Table storing the Intermediate results this joinable pair creates.

    // When the plan generator creates an assignment, which creates this
    // Joinable Pair as an intermediate result , store it in this variable.
    private IntermediateResultAssignment assignment;

    public JoinablePair(JoinableExpression left, JoinableExpression right) {
        this.left = left;
        this.right = right;
        this.columnsContainingKeywords = new HashSet<>();
        this.columnsContainingKeywords.addAll(left.getColumnsContainingKeywords());
        this.columnsContainingKeywords.addAll(right.getColumnsContainingKeywords());
        this.assignment = null;
        this.table = null;
    }    

    // Getters and Setters.
    public JoinableExpression getLeft()  { return left; }
    public JoinableExpression getRight() { return right;}    
    public IntermediateResultAssignment getAssignment() { return assignment; }

    public void setLeft(TupleSet left)  { this.left = left; }
    public void setRight(TupleSet right) { this.right = right;}
    
    // Once the Temp Table created by the Executor is created set it as this.table.
    public void setTable(SQLTable table) { this.table = table;}   

    // Swaps the left and right joinable expressions (for testing).
    public void swap() {
        JoinableExpression temp = this.left;
        this.left = this.right;
        this.right = temp;
    }

    // Returns true if it left and right Expressions are FreeTupleSets
    public boolean isCompliedByFreeTupleSets() {
        return (this.left instanceof FreeTupleSet) && (this.right instanceof FreeTupleSet);
    }


    // Returns true if it contains the IntermediateResultAssignment or is created by it.
    public boolean containsOrCreatedByIntermediateResult(IntermediateResultAssignment intermediateResult) {
        // Check if this.assignment is equal with intermediateResult and return result.
        if (this.assignment != null)
            return this.assignment.equals(intermediateResult);
        // Else check if this JoinablePair Contains this intermediateResult.
        else         
            return this.containsIntermediateResult(intermediateResult);
    }

    // Returns true if it contains the IntermediateResultAssignment. We are not asking about 
    // the intermediate result assignment creating this Joinable pair , which is this.assignment.
    // But if the left or right JoinableExpressions contain it.
    @Override
    public boolean containsIntermediateResult(IntermediateResultAssignment intermediateResult) {
        return this.left.containsOrCreatedByIntermediateResult(intermediateResult) || 
               this.right.containsOrCreatedByIntermediateResult(intermediateResult) ;
    }

    // Remove intermediateResultAssignment
    @Override
    public boolean removeIntermediateResultAssignment(IntermediateResultAssignment intermediateResult) {
        // If it is created by that assignment remove it and return true.
        if (this.assignment != null && this.assignment.equals(intermediateResult)) { 
            this.assignment = null;
            return true;
        }
        // Else check left and right expressions and remove from them.
        return this.left.removeIntermediateResultAssignment(intermediateResult) ||
               this.right.removeIntermediateResultAssignment(intermediateResult) ;

    }
    
    // This JoinablePair is assigned as an IntermediateResult, set the Assignment creating the IR.
    public void setAssignmentCreatingIntermediateResult(IntermediateResultAssignment termCreatingExpression) {        
        this.assignment = termCreatingExpression;
    }

    // Return the columns that this Intermediate result contributes to the result.
    @Override
    public Set<SQLColumn> getColumnsContainingKeywords() {
        return this.columnsContainingKeywords;
    }

    // Return the Temp SQLTable created to store this intermediate result.
    @Override
    public SQLTable getTable() {
        return this.table;
    }

    @Override
    public String toAbbreviation() {
        String str = new String();
        if (this.left instanceof JoinablePair) {
            str += ((((JoinablePair) this.left).assignment == null)?
                (this.left.toAbbreviation()) :
                ( ((JoinablePair) this.left).assignment.toAbbreviation() )
            );
        } else str += this.left.toAbbreviation();

        str += ", ";

        if (this.right instanceof JoinablePair) {
            str += ((((JoinablePair) this.right).assignment == null)?
                (this.right.toAbbreviation()) :
                ( ((JoinablePair) this.right).assignment.toAbbreviation() )
            );
        } else str += this.right.toAbbreviation();

        return str;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash += 31 * (this.left == null ? 0 : this.left.hashCode());
        hash += 31 * (this.right == null ? 0 : this.right.hashCode());
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof JoinablePair)) return false;
        JoinablePair pair = (JoinablePair) obj;

        return (pair.left.equals(this.left) && pair.right.equals(this.right)) ||
               (pair.left.equals(this.right) && pair.right.equals(this.left));
    }

    @Override
    public String toString() {
        return "<" + this.left.toAbbreviation() + ", " + this.right.toAbbreviation() + ">";
    }

    @Override
    public Set<String> getContainedBaseTables() {
        Set<String> tables  = new HashSet<>();

        // Use the functions on both the left and right
        tables.addAll(this.left.getContainedBaseTables());
        tables.addAll(this.right.getContainedBaseTables());

        return tables;
    }

}
