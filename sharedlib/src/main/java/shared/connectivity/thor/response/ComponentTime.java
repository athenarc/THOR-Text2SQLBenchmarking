package shared.connectivity.thor.response;

import java.text.DecimalFormat;

/** 
 * This class represents the execution time of a component of the system.
 */
public class ComponentTime {

    private String componentName; // The name of the component.
    private Double time;
    // private Double percentage;

    /**
     * Constructor.
     */
    public ComponentTime(String componentName, Double time) {
        this.componentName = componentName;
        
        // Format the time (round to 2 decimal points).
        DecimalFormat df = new DecimalFormat("#0.##");
        this.time = Double.valueOf(df.format(time));
    }

    /**
     * @return The name of the component.
     */
    public String getComponentName() {
        return this.componentName;
    }

    /**
     * @return The execution time of the component.
     */
    public Double getTime() {
        return this.time;
    }

    /**
     * @param time the time to set
     */
    public void setTime(Double time) {
        DecimalFormat df = new DecimalFormat("#0.##");
        this.time = Double.valueOf(df.format(time));
    }

    // public Double getPercentage() {
    //     return this.percentage;
    // }

    // public void setPercentage(Double percentage) {
    //     this.percentage = percentage;
    // }

}