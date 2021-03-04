package shared.database .model;

// This class models the value of an SQL attribute (Float type).
public class SQLFloatValue extends SQLValue {

    private Float value;

    public SQLFloatValue(Float value) {
        super("float", 0);
        this.value = value;
    }

    // Getters and Setters.
    public Float getValue() {
        return this.value;
    }

    public void setValue(Float value) {
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
        if (!(obj instanceof SQLFloatValue)) return false;

        // Typecast obj to SQLFloatValue so that we can compare the data members.
        SQLFloatValue c = (SQLFloatValue) obj;

        // Compare values
        return this.value.equals(c.value);
    }

    @Override
    public String toString() {
        if (this.value == null) return NULL_VALUE_STRING;
        return this.getValue().toString();
    }

}
