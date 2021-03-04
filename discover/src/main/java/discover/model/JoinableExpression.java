package discover.model;

import java.util.Set;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import discover.model.execution.IntermediateResultAssignment;


public interface JoinableExpression {
    
    // Get the intermediate Result contained in this JoinableExpression.
    public boolean containsIntermediateResult(IntermediateResultAssignment intermediateResult);

    // Get the intermediate Result contained in this JoinableExpression or creating the JoinableExpression.
    public boolean containsOrCreatedByIntermediateResult(IntermediateResultAssignment intermediateResult);

    // Remove containedIntermediateResult from this JoinableExpression if it is used to create it.
    public boolean removeIntermediateResultAssignment(IntermediateResultAssignment intermediateResult);    


    // Returns an SQLTable.
    // - If the Joinable Expression is a TupleSet
    //   return the SQLTable that contains it.
    // - If the Joinable Expression is a JoinablePair
    //   (an intermediate Result) then return the Temp
    //   SQLTable that was created by the Plan Executor.
    public SQLTable getTable();

    public Set<String> getContainedBaseTables();

    // Set the tableCreating the JoinableExpression.
    public void setTable(SQLTable table);

    // Gets the columns containing the Keywords that this joinable
    // expression contributes to the result.
    public Set<SQLColumn> getColumnsContainingKeywords();
    
    @Override
    public boolean equals(Object obj);

    @Override
    public int hashCode();

    public String toAbbreviation();

}
