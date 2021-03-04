package discover.model.execution;

import discover.model.JoinableExpression;

// A class extending the Abstract Assignment class. This 
// kind of Assignment is responsible for creating the 
// Intermediate Results produced but the CNGenerator.
public class IntermediateResultAssignment extends Assignment {

    private static int nextId = 0; // A static int holding the number of CNs generated.
    private static String intermediatePrefix = "T"; // A prefix for printing reasons.    

    public IntermediateResultAssignment(JoinableExpression assignmentTerms) {
        super(IntermediateResultAssignment.nextId, intermediatePrefix, assignmentTerms);
        IntermediateResultAssignment.incrementNextId();
    }

    // A static method Incrementing the static ID.
    public static void incrementNextId() {
        IntermediateResultAssignment.nextId++;
    }   

}