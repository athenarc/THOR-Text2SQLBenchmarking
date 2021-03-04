package expressq2.model;

import java.util.ArrayList;
import java.util.List;

/**
 * This class models a group of tags. All tag contained in a group. 
 * reference the same relation or object node in the ORMSchemaGraph. 
 */
public class TagGrouping {    
    List<Tag> tags;          // The tags contained in the group.
    List<Keyword> keywords;  // The keywords referring to this TagGrouping.


    /**
     * Public constructor. 
     */
    public TagGrouping(Tag tag, Keyword keyword) {
        this.tags = new ArrayList<>();
        this.keywords = new ArrayList<>();
        this.tags.add(tag);
        this.keywords.add(keyword);
    }

    /**
     * Get TagGrouping Label. 
     */
    public String getLabel() {
        if (!tags.isEmpty())
            return this.tags.get(0).getLabel(); 
        else 
            return null;
    }

    /**
     * @return the keywords
     */
    public List<Keyword> getKeywords() {
        return keywords;
    }

    /*
     * Return all Tags. 
     */
    public List<Tag> getTags() {
        return tags;
    }

    /* 
     * Add a Tag with its respective  keyword.
     */
    public boolean addTag(Tag tag, Keyword keyword) {
        return this.tags.add(tag) && this.keywords.add(keyword);
    }

    /**
     * Return true if TagsGroup contains a tag with the same attr and label as the parameter tag. 
     * Parameters : Tag tag , the tag to compare to.
     */
    public boolean containsTagWithEqualAttr(Tag tag) {
        // Find an contained tag with the same attribute as tag.
        boolean matchingAttrs = false;
        for (Tag containedTag: this.tags)
            if (containedTag.getAttr() != null && containedTag.getAttr().equals(tag.getAttr()))
                matchingAttrs = true;

        // Return the boolean.
        return this.getLabel().equals(tag.getLabel()) && matchingAttrs;
    }

    /**
     * Return true if TagsGroup contains a tag with the same attr (which also contains a condition )and label as the parameter tag. 
     * Parameters : Tag tag , the tag to compare to.
     */
    public boolean containsTagWithEqualAttr_wCond(Tag tag) {
        // Find an contained tag with the same attribute as tag.
        boolean matchingAttrs = false;
        for (Tag containedTag: this.tags)
            if (containedTag.getAttr() != null && containedTag.getCond() != null && containedTag.getAttr().equals(tag.getAttr()))
                matchingAttrs = true;

        // Return the boolean.
        return this.getLabel().equals(tag.getLabel()) && matchingAttrs;
    }

    /**               
     * @return Returns a boolean indicating if this TagGrouping 
     * contains a Tag that refers to a component Relation.
     */
    public boolean refersToComponentRelation() {
        for (Tag tag: this.tags)
            if (tag.refersToComponentRelation()) 
                return true;
        return false;
    }

    /**               
     * @return the ComponentRelation referred by this TagGrouping.
     */
    public String getReferredComponentRelation() {
        for (Tag tag: this.tags)
            if (tag.refersToComponentRelation()) 
                return tag.getComponentRelationName();
        return null;
    }

    
    /**     
     * Returns true if this TagGrouping contains a Tag that has 
     * its attribute field not null.
     * 
     * @return True if this TagGrouping refers to an attribute.
     */
    public boolean refersToAttribute() {
        for (Tag tag: this.tags)
            if (tag.refersToAttribute()) 
                return true;
        return false;
    }

    /**               
     * @return the Attribute referred by this TagGrouping.
     */
    public String getReferredAttribute() {
        for (Tag tag: this.tags)
            if (tag.refersToAttribute()) 
                return tag.getAttr();
        return null;
    }

    /**     
     * Returns true if this TagGrouping contains a Tag that has 
     * its Label field not null.
     * 
     * @return True if this TagGrouping refers to a Relation.
     */
    public boolean refersToRelation() {
        for (Tag tag: this.tags)
            if (tag.refersToRelation()) 
                return true;
        return false;
    }

    /**     
     * @return a List of Tags from the grouping that contain a condition
     */
    public List<Tag> getTagsWithCondition() {
        List<Tag> tagsWithCond = new ArrayList<>();
        for (Tag tag: this.tags)
            if (tag.getCond() != null) 
                tagsWithCond.add(tag);
        return tagsWithCond;
    }

    /**
     * Print the TagGrouping.
     */
    @Override
    public String toString() {
        return this.tags.toString();
    }

}