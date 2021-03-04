package shared.database .model;

// This class models the value of an SQL attribute (VARCHAR type).
public class SQLVarcharValue extends SQLValue {

    private String value;

    public SQLVarcharValue(String value, Integer maximumLength) {
        super("varchar", maximumLength);
        this.value = value;
    }

    public SQLVarcharValue(String value, Long maximumLength) {
        super("varchar", maximumLength);
        this.value = value;
    }

     // Returns the length of the value in words (for text attributes only).
     @Override
     public int getLength() {
         return this.value.trim().split("\\s+").length;
     }

    // Getters and Setters.
    @Override
    public String getValue() {
        return this.value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    // Returns true if the keyword appears in the string value (case insensitive).
    @Override
    public boolean contains(String keyword) {
        if (this.value == null) {
            return false;
        }
        else {
            return this.value.toLowerCase().contains(keyword.toLowerCase());
        }
    }   

    @Override
    public int hashCode() {
        return this.value.hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof SQLVarcharValue)) return false;
        // Typecast obj to SQLVarcharValue so that we can compare the data members.
        SQLVarcharValue c = (SQLVarcharValue) obj;

        // If value is null then return false.
        if (this.value == null) {
            return false;
        }

        // Compare the values.
        return this.value.equals(c.value);
    }

    @Override
    public String toString() {
        if (this.value == null) return SQLValue.NULL_VALUE_STRING;
        return this.value;
    }

}
