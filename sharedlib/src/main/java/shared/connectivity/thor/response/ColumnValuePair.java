package shared.connectivity.thor.response;

/**
 * This class represents a <Column, Value> pair of a result.
 * A list of these pairs constituted a result (tuple).
 */
public class ColumnValuePair {

    private String column; // The name of the column.
    private String table; // The name of table the column belongs to. Can be null in case of aggregate queries.
    private Object value; // The value.

    /**
     * Constructor.
     */
    public ColumnValuePair(String column, String table, Object value) {
        this.column = column;
        this.table = table;
        this.value = value;
    }

     /**
     * Constructor without table field.
     */
    public ColumnValuePair(String column, Object value) {
        this.column = column;
        this.table = null;
        this.value = value;
    }

    /**
     * @return The name of the column.
     */
    public String getColumn() {
        return this.column;
    }

    /**
     * @return The name of table the column belongs to. Can be null in case of aggregate queries.
     */
    public String getTable() {
        return this.table;
    }

    /**
     * @return The value.
     */
    public Object getValue() {
        return this.value;
    }

}