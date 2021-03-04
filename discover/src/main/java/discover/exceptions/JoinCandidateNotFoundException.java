package discover.exceptions;

import java.util.List;

import shared.database.model.SQLTable;
import discover.model.JoinableExpression;

// This exception indicates that there was no join candidate found
// for e joinable expression.
public class JoinCandidateNotFoundException extends Exception {

    private static final long serialVersionUID = -2469597857312506587L;

    // Public Constructors    
    public JoinCandidateNotFoundException(JoinableExpression joinableExpression) {
        super(
            "[ERR] JoinableExpression \"" +
            joinableExpression.toAbbreviation() + 
            "\" has no join candidate."
        );
    }
    
    public JoinCandidateNotFoundException(SQLTable table, List<SQLTable> candidates) {
        super("[ERR] Cant join table \"" +
            table.getName() + "\" with candidate Tables \"" +
            candidates.toString() + "\""
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

