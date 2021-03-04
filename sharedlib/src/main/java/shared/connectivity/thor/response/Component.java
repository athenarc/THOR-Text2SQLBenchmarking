package shared.connectivity.thor.response;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import shared.util.Table;
import shared.util.Timer;
import shared.util.Timer.Type;


/**
 * This class represents a component and stores information (related to the component)
 * which will be displayed in the front end.
 */
public class Component {

    private static Integer COMPONENT_COUNT = 0;

    private Integer id;    // The id of the component.
    private String name;   // The name of the component.
    private Double time;   // The execution time of the component.
    private Timer timer;   // The timer used to count the components elapsed time.
    private List<Table> componentInfo;  // The information of the component.

    // A list of components that are connected with the current one in the
    // system's directed architecture graph and their corresponding labels.
    // Only out going edges are considered.
    private List<Component> outGoingConnections;
    private List<String> outGoingLabels;

    /** 
     * Constructor 
     */
    public Component(String name) {
        if (name.isEmpty()) { this.name = "unknown"; }
        else { this.name = name; }

        this.id = COMPONENT_COUNT++;
        this.componentInfo = new ArrayList<>();
        this.outGoingConnections = new ArrayList<>();
        this.outGoingLabels = new ArrayList<>();
        this.time = 0D;
        this.timer = new Timer(Type.WALL_CLOCK_TIME);
    }

    /**
     * Connects the current component with the provided component, with a directed edge in the
     * system's architecture graph. The edge is directed from the current component to the
     * provided one, to indicate that the output of the former serves as an input for the latter.
     * 
     * @param destination The destination component.
     * @param label The label of the edge.
     */
    public void connectWith(Component destination, String label) {
        this.outGoingConnections.add(destination);
        this.outGoingLabels.add(label);
    }

    /**
     * @return The id of the component.
     */
    public String getId() {
        return this.name.charAt(0) + id.toString();
    }

    /**
     * @return The name of the component.
     */
    public String getName() {
        return name;
    }

    /**
     * @return The execution time of the component.
     */
    public Double getTime() {
        return time;
    }

    /**
     * @return The information of the component.
     */
    public List<Table> getComponentInfo() {
        return componentInfo;
    }

    /**
     * @return The out going connections of the component.
     */
    public List<Component> getOutGoingConnections() {
        return outGoingConnections;
    }

    /**
     * @return The labels fo the out going connections of the component.
     */
    public List<String> getOutGoingLabels() {
        return outGoingLabels;
    }

    /**
     * @param name The name of the component.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @param time The execution time of the component.
     */
    public void setTime(Double time) {
        // Format the time (round to 2 decimal points).
        DecimalFormat df = new DecimalFormat("#0.##");
        this.time = Double.valueOf(df.format(time));
    }

    /**
     * Starts the Timer, used to measure the components execution time.
     */
    public void startTimer(){
        this.timer.start();
    }

    /**
     * Stops the Timer, used to measure the components execution time.
     */
    public void stopTimer(){
        this.time = this.timer.stop();
    }

    /**
     * @param componentInfo The information of the component.
     */
    public void setComponentInfo(List<Table> componentInfo) {
        this.componentInfo = componentInfo;
    }

    /**
     * Adds new information for the component.
     * 
     * @param componentInfo The information to add.
     */
    public void addComponentInfo(Table componentInfo){
        this.componentInfo.add(componentInfo);
    }

}