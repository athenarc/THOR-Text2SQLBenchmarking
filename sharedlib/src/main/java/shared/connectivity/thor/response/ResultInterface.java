package shared.connectivity.thor.response;

import java.util.Collection;

/**
 * This interface must be implemented by a class that represents a result (tuple) of the system.
 */
public interface ResultInterface {

    /**
     * A list that contains the <Column, Value> pairs of the result (tuple).
     */
    public Collection<ColumnValuePair> getColumnValuePairs();

    /**
	 * A list with the names of the tables that were joined to produce the result.
     */
    public Collection<String> getNetworks();

    /**
     * The SQL query that was executed to produce the result.
     */
    public String getQuery();

    /**
     * Returns true if the system assigns a score to every result.
     */
    public boolean hasScoreField();

    /**
     * Returns the result's score if one is present.
     */
    public double getResultScore();

}