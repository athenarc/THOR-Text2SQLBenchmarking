package expressq2.model;


/**
 * This class models a Tag used in the Query Analysis component
 * of the System. Each keyword is assigned with one Tag.
 * A Tag holds information about the :
 *   1. The name of the object or relationship that this keyword relates to. (label)
 *   2. The attribute of the above object or relationship. (attr)
 *   3. The restriction on the above object or relationship. (cond) 
 */
public class Tag { 

    private String label;  // Relation Name
    private String attr;   // Attribute Name
    private String cond;   // Specific value.

    private int tagAppearances;           // How mane times does to Tag Appear in the database.
    private String componentRelationName; // Indicates whether the Tag Refers to a ComponentRelation    

    // This boolean indicates whether a Tag created for a specific Keyword has a penalty.
    // A Tag gets assigned with a penalty when creating a List of Tags for the specific Keyword.
    // If this list of Tags contains a Tag linking the keyword with a Relation/Attribute name 
    // then all other Tags created for that keyword have a penalty.
    private boolean hasPenalty;

    /** Getters And Setters */
    public String getAttr() { return attr;}    
    public String getCond() { return cond; }
    public String getLabel() { return label; }

    public void setAttr(String attr) { this.attr = attr; }
    public void setCond(String cond) { this.cond = cond; }
    public void setLabel(String label) { this.label = label; }


    /** Public Constructor of a Tag referring to a Relation Name */
    public Tag(String label) {
        this.label = label;
        this.attr = null; 
        this.cond = null;
        this.tagAppearances = 1;
        this.componentRelationName = null;
    }

    /** Public Constructor of a Tag referring to a Relation's Atttribute Name */
    public Tag(String label, String attr) {
        this.label = label;
        this.attr = attr; 
        this.cond = null;
        this.tagAppearances = 1;
        this.componentRelationName = null;
    }

    /** Public Constructor of a Tag referring to a Relations value on a specific Attribute */
    public Tag(String label, String attr, String value, int appearances) {
        this.label = label;
        this.attr = attr; 
        this.cond = value;
        this.tagAppearances = appearances;
        this.componentRelationName = null;
    }


    /**
     * @param hasPenalty the hasPenalty to set
     */
    public void setPenalty(boolean hasPenalty) {
        this.hasPenalty = hasPenalty;
    }

    /**
     * @return A boolean indicating if the Tag has Penalty.
     */
    public boolean hasPenalty() {
        return this.hasPenalty;
    }

    /**
     * @param componentRelationName the componentRelationName to set
     */
    public void setComponentRelationName(String componentRelationName) {
        this.componentRelationName = componentRelationName;
    }
    

    /**     
     * @return A boolean indicating if the Tag Refers to a component Relation.
     */
    public boolean refersToComponentRelation() {
        return this.componentRelationName != null;
    }


    /**    
     * @return The componentRelation's name, if it refers to one.
     */
    public String getComponentRelationName() {
        return this.componentRelationName;
    }


    /**     
     * Returns true if this Tag has its attribute field not null,
     * and its condition field null.
     * 
     * @return True if this Tag refers to an attribute.
     */
    public boolean refersToAttribute() {        
        if (this.attr != null && this.cond == null) 
            return true;
        else 
            return false;
    }

    /**     
     * Returns true if this Tag has its label field not null. 
     * And no other field not null.
     * 
     * @return True if this Tag refers to a Relation.
     */
    public boolean refersToRelation() {
        if (this.label != null && this.attr == null && this.cond == null) 
            return true;
        else 
            return false;
    }


    /**
     * @return An int indicating how many entities where found in the Database 
     * connecting with this Tag.
     */
    public int getAppearances() {
        return this.tagAppearances;
    }


    @Override
    public String toString() {
        return "(" + this.label + ", " + this.attr + ", " + this.cond + ")";
    }

}