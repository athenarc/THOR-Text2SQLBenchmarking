package expressq2.model;

import shared.database.model.SQLColumn;


/**
 * This class Models a condition annotation. A condition annotation appears in a 
 * Query PatterNode and indicataes that the node carries a condition. This 
 * condition applies to the node at the time of translating the QueryPattern to 
 * a query. The condition indicates that the node annotated with it matches to 
 * a specific entity in the database.
 */
public class ConditionAnnotation {
    public SQLColumn attribute;   // The SQLColumn on which the value appears.
    public String value;          // The value of the Condition.
    public int appearances;       // The number of appearances of the above value in the SQLColumn.

    /** Constructor */
    public ConditionAnnotation(SQLColumn attribute, String value, int appearances) {
        this.attribute = attribute;
        this.value = value;
        this.appearances = appearances;
    }

    /**
     * @return the attribute
     */
    public SQLColumn getAttribute() {
        return attribute;
    }    

    /**
     * @return the value
     */
    public String getValue() {
        return value;
    }

    /**
     * @return the appearances
     */
    public int getAppearances() {
        return appearances;
    }

    /** Print EqualityAnnotation */
    @Override
    public String toString() {
        return this.attribute.getName() + " = " + this.value;
    }
}