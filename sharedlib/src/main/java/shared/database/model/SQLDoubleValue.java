package shared.database .model;

// This class models the value of an SQL attribute (Double type).
public class SQLDoubleValue extends SQLValue {

    private Double value;

    public SQLDoubleValue(Double value) {
        super("double", 0);
        this.value = value;
    }

    // Getters and Setters.
    public Double getValue() {
        return this.value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    // Returns false by default.
    @Override
    public boolean contains(String keyword) {
        return false;
    }

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        // If the object is compared with itself then return true.
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof SQLDoubleValue)) return false;

        // Typecast obj to SQLDoubleValue so that we can compare the data members.
        SQLDoubleValue c = (SQLDoubleValue) obj;

        // Compare values
        return this.value.equals(c.value);
    }

    @Override
    public String toString() {
        if (this.value == null) return NULL_VALUE_STRING;
        return this.getValue().toString();
    }

}
