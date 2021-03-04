package discover.model.execution;

import shared.database.model.SQLColumn;
import shared.util.Pair;

// Represents a equation used to join two tables.
//     [table_identifier].[pk_col_name] = [table_identifier].[fk_col_name]
public class JoinEquation {

    Pair<SQLColumn, String> lhs; // Its the part : [table_identifier].[pk_col_name].
    Pair<SQLColumn, String> rhs; // Its the part : [table_identifier].[fk_col_name].

    public JoinEquation(SQLColumn pkColumn, String pkTableAlias, SQLColumn fkColumn, String fkTableAlias) {
        this.lhs = new Pair<>(pkColumn, pkTableAlias);
        this.rhs = new Pair<>(fkColumn, fkTableAlias);
        
    }

    @Override
    public int hashCode() {
        return lhs.hashCode() + rhs.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof JoinEquation)) return false;
        JoinEquation je = (JoinEquation) obj;
        return (lhs.equals(je.lhs) && rhs.equals(je.rhs)) ||
            (lhs.equals(je.rhs) && rhs.equals(je.lhs));
    }

    @Override
    public String toString() {
        return 
            this.lhs.getRight() + "."  + this.lhs.getLeft().getName() + 
            " = " +
            this.rhs.getRight() + "."  + this.rhs.getLeft().getName() ;
    }

}