package shared.database.model;

import java.util.List;

/**
 * This class represents a result returned by
 * {@link shared.database.connectivity.DatabaseIndexManager} for a keyword. It
 * includes a list of {@link SQLTuple}. The SQLTuples in the list containing
 * only 2 attributes and 2 values The first attribute is the Primary Key and the
 * second is the column where the keyword was found.
 * 
 * Note : Each SQLTuple in this list contains the same second attribute (column)
 * because they are results from a search of the DatabaseIndexManager on a specific column. 
 */
public class SQLIndexResult {

    String keyword;          // It is the keyword used to return this result from Database's inverted indexes
    List<SQLTuple> tuples;   // The SQLTuples containing the above keyword.

    /**
     * Public Constructor
     */
    public SQLIndexResult(String keyword, List<SQLTuple> tuples) {
        this.keyword = keyword;
        this.tuples = tuples;
    }

    /**
     * @return The column containing the keyword used by 
     * {@link shared.database.connectivity.DatabaseIndexManager} to produce this result.
     */
    public String getColumnContainingKeyword() {
        String column = new String();
        
        if (tuples != null && tuples.size() != 0) {
            // We know by definition that the second attribute 
            // in each tuple is the wanted column.
            column = this.tuples.get(0).attributes.get(1).toString();
        }

        return column;
    }


    /**
     * @return the tuples
     */
    public List<SQLTuple> getTuples() {
        return tuples;
    }

}