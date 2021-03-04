package shared.connectivity.thor.response;

import shared.util.Table;
import java.util.List;

/** 
 * This class represents the statistics of a component of the system.
 * The statistics are modeled as a table, and displayed as such in the front end.
 */
public class ComponentStats {

    private String componentName;
    private List<Table> componentInfo;

    /**
     * Constructor.
     */
    public ComponentStats(String componentName, List<Table> componentInfo) {
        this.componentName = componentName;
        this.componentInfo = componentInfo;
    }

    /**
     * @return The name of the component.
     */
    public String getComponentName() {
        return this.componentName;
    }

    /**
     * @return The statistics of the component in a table format.
     */
    public List<Table> getComponentInfo() {
        return this.componentInfo;
    }

}