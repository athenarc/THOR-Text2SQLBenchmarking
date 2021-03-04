package spark.exceptions;

import shared.database.model.SQLTable;
import spark.model.JoiningNetworkOfTupleSets;
import spark.model.TupleSet;

// This exception indicates that there was no join candidate found
// for e joinable expression.
public class JoinCandidateNotFoundException extends Exception {

    private static final long serialVersionUID = -2469597857312506587L;

    // Public Constructors    
    public JoinCandidateNotFoundException(JoiningNetworkOfTupleSets joinableExpression) {
        super(
            "[ERR] JoinableExpression \"" +
            joinableExpression.toAbbreviation() + 
            "\" has no join candidate."
        );
    }

    // Public Constructors    
    public JoinCandidateNotFoundException(TupleSet joinableExpression) {
        super(
            "[ERR] JoinableExpression \"" +
            joinableExpression.toAbbreviation() + 
            "\" has no join candidate."
        );
    }

    // Public Constructors    
    public JoinCandidateNotFoundException(SQLTable leftTable, SQLTable rightTable) {
        super(
            "[ERR] Cannot Create Join : \"" +
            leftTable.getName() + "\" |><| \"" + 
            rightTable.getName() +            
            "\" . Tables have no joining columns."
        );
    }

    public JoinCandidateNotFoundException(String message, Throwable cause) { super(message, cause); }
    public JoinCandidateNotFoundException(Throwable cause) { super(cause); }
}

