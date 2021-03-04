package shared.database.model;

// This class models the value of an SQL attribute.
// It is extended by specific SQL value types.
public abstract class SQLValue {

    protected static final String NULL_VALUE_STRING = "NULL";

    private SQLType type; // The type of the value's attribute.

    public SQLValue(String type, Integer maximumLength) {
        this.type = new SQLType(type, maximumLength);
    }

    public SQLValue(String type, Long maximumLength) {
        this.type = new SQLType(type, maximumLength);
    }

    // Getters and Setters.
    public SQLType getType() {
        return this.type;
    }

    public void setType(String type, Integer maximumLength) {
        this.type = new SQLType(type, maximumLength);
    }
    
    // Returns true if a keyword appears in the value.
    public abstract boolean contains(String keyword);

    // Returns the length of the value in words (text attribute values override this function).
    public int getLength() { return 0;}

    // Returns the actual value.
    public abstract Object getValue();

    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

}
