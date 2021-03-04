package shared.connectivity.thor.response;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/** 
 * This class represents a single result of the system.
 * Contains the values of the tuple, and additional information for display purposes.
 * It is parametrized with a class that implements the ResultInterface, which models a single result.
 */
public class Result<R extends ResultInterface> {

    private List<ColumnValuePair> data; // The values of the tuple.
    private List<String> network; // The network that produced the result.
    private String query; // The SQL query that produced the result.

    /**
     * Constructor.
     */
    public Result(R systemsResult) {
        this.data = new ArrayList<>(systemsResult.getColumnValuePairs());

        // Insert the score column independently since it is not part of the actual attributes.
        if (systemsResult.hasScoreField()) {
            // Format the score (round to 2 decimal points).
            DecimalFormat df = new DecimalFormat("#0.##");
            this.data.add(new ColumnValuePair("score", Double.valueOf(df.format(systemsResult.getResultScore()))));
        }

        this.network = new ArrayList<>(systemsResult.getNetworks());
        this.query = systemsResult.getQuery();
    }

    /**
     * @return The list of <Column, Value> pairs if the result (tuple).
     */
    public List<ColumnValuePair> getData() {
        return this.data;
    }

    /**
     * @return The network that produced the result.
     */
    public List<String> getNetwork() {
        return this.network;
    }

    /**
     * @return The SQL query that produced the result.
     */
    public String getQuery() {
        return this.query;
    }

}