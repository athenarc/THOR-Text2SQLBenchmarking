package shared.connectivity.thor.response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Interpretation<I extends InterpretationInterface> {
    

    private List<String> columns;       // The columns of the Table. Basically the attributes of the query results.
    private List<List<String>> rows;    // The rows of the Table. Basically the values of the query results.
    private List<String> network;       // The network that produced the result.
    private String query;               // The SQL query that produced the result.

    /**
     * Constructor.
     */
    public Interpretation(I systemsResult) {        
        this.columns = new ArrayList<>();
        if (systemsResult.getAttributeNames() != null) this.columns.addAll(systemsResult.getAttributeNames());

        this.rows = new ArrayList<List<String>>();
        for (Collection<String> r: systemsResult.getRows())
            this.rows.add(new ArrayList<String>(r));

        this.network = new ArrayList<>();
        if (systemsResult.getNetworks() != null) this.network.addAll(systemsResult.getNetworks());
        
        this.query = systemsResult.getQuery();
    }

    /**
     * @return The list of <Column, Value> pairs if the result (tuple).
     */
    public List<String> getColumns() {
        return this.columns;
    }

    /**
     * @return The list of <Column, Value> pairs if the result (tuple).
     */
    public List<List<String>> getRows() {
        return this.rows;
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