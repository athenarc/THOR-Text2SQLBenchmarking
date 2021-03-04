package shared.database .model;

// This class models the value of an SQL attribute (INT type).
public class SQLIntValue extends SQLValue {

    private Integer value;

    public SQLIntValue(Integer value) {
        super("int", 0);
        this.value = value;
    }

    // Getters and Setters.
    @Override
    public Integer getValue() {
        return this.value;
    }

    public void setValue(Integer value) {
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
        if (!(obj instanceof SQLIntValue)) return false;

        // Typecast obj to SQLIntValue so that we can compare the data members.
        SQLIntValue c = (SQLIntValue) obj;

        // Compare values
        return this.value.equals(c.value);
    }

    @Override
    public String toString() {
        if (this.value == null) return NULL_VALUE_STRING;
        return this.getValue().toString();
    }

}
