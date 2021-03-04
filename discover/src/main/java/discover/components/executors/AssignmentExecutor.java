package discover.components.executors;

// Input : An Assignment.
// Output : Depends on the assignment type.
//         - None for Intermediate Result creation assignments.
//         - The tuples contained in the Candidate Network for
//           Candidate Network creation assignments.
// An Abstract class holding useful functions that 
// the sub classes extending it use. Its execute Function
// is implemented by the classes extending it.
public abstract class AssignmentExecutor {}