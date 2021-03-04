package expressq2.model;

import java.util.HashSet;
import java.util.Set;

/**
 * This class stores the SQLQuery to string format and the tables used to create
 * this query.
 */
public class SQLQuery {
    private String query;               // The Query.
    private Set<String> tables;         // The tables used in the above query.

    /** Public constructor */
    public SQLQuery() {     
        this.tables = new HashSet<>();
    }    

    /**
     * Getters and Setters
     */
    public String getQueryToString() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Set<String> getTables() {
        return this.tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public void addTable(String table) {
        this.tables.add(table);
    }

    @Override
    public String toString() {
        return this.getQueryToString();
    }
}