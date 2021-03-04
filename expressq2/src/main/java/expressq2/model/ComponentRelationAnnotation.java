package expressq2.model;

/**
 * The component Relation Annotation models an annotation for an ORMNode.
 * This kind of annotation indicated that an ORMNode's Component Relation is 
 * been searched by the user in the query so we expand the Entity represented
 * by the ORMNode with the Component Relation of this annotation.
 */
public class ComponentRelationAnnotation {

    String componentRelation;   // The component Relation's name

    public ComponentRelationAnnotation(String componentRel) {
        this.componentRelation = componentRel;
    }

    /**
     * @return the componentRelation
     */
    public String getComponentRelation() {
        return componentRelation;
    }

    @Override
    public String toString() {
        return this.componentRelation;
    }
    
    @Override
    public int hashCode() {
        return this.componentRelation.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof ComponentRelationAnnotation)) return false;
        ComponentRelationAnnotation c = (ComponentRelationAnnotation) obj;
        return this.componentRelation.equals(c.componentRelation);
    }

}