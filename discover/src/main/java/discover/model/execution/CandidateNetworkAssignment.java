package discover.model.execution;

import discover.model.JoinableExpression;

// A class extending the Abstract Assignment class. This 
// kind of Assignment is responsible for creating the 
// Candidate Networks produced but the CNGenerator.
public class CandidateNetworkAssignment extends Assignment {

    private static int nextId = 0; // A static int holding the number of CNs generated.
    private static final String networkPrefix = "C"; // A prefix for printing reasons.

    public CandidateNetworkAssignment(JoinableExpression assignmentTerms) {
        super(CandidateNetworkAssignment.nextId, networkPrefix, assignmentTerms);
        CandidateNetworkAssignment.incrementNextId();
    }

    // A static method Incrementing the static ID.
    public static void incrementNextId() {
        CandidateNetworkAssignment.nextId++;
    }   

}