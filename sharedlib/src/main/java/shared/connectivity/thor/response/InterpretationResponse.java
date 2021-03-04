package shared.connectivity.thor.response;

import java.util.List;
import java.util.ArrayList;
import java.text.DecimalFormat;

import com.google.gson.Gson;

/** 
 * This class represents the response of the system to a query.
 * Contains all the information that will be displayed in the front end of the application.
 * It is parametrized with a class that implements the ResultInterface, which models a single result.
 */
public class InterpretationResponse<I extends InterpretationInterface> {

    private String id;                                 // The id of the system (the name in lowercase and without spaces).
    private String name;                               // The name of the system.    
    private List<ComponentTime> componentsTime;        // Saves the time taken by every component of the system.
    private List<ComponentStats> componentStats;       // Saves the statistics of every component of the system
    private double totalTime;                          // The total running time of the system.
    private List<Interpretation<I>> interpretations;   // The top results returned by the system.


    /**
     * * Initialize this Interpretations response using a list of {@link Component}s.
     * 
     * @param id
     * @param name
     * @param components
     * @param interpretations
     */
    public InterpretationResponse(String id, String name, List<Component> components, List<I> interpretations) {
        this.id = id;
        this.name = name;        
        this.componentsTime = new ArrayList<>();
        this.componentStats = new ArrayList<>();
        this.totalTime = 0.0;
        

        for (Component component : components) {
            // Save the execution time of the component.
            this.componentsTime.add(new ComponentTime(component.getName(), component.getTime()));
            
            // Add the time to the total execution time.
            this.totalTime += component.getTime();

            // Save the statistics of the component.
            this.componentStats.add(new ComponentStats(component.getName(), component.getComponentInfo()));
        }
        
        // Format the total time (round to 2 decimal points).
        DecimalFormat df = new DecimalFormat("#0.##");
        this.totalTime = Double.valueOf(df.format(this.totalTime));

                            
        // Create a Result object for every tuple.
        this.interpretations = new ArrayList<Interpretation<I>>();
        for (I sysInterpretation : interpretations) {
            this.interpretations.add(new Interpretation<>(sysInterpretation));
        }
    }

    /**
     * Initialize this Interpretations response using total time.
     * 
     * @param id
     * @param name
     * @param totalTime
     * @param interpretations
     */
    public InterpretationResponse(String id, String name, double totalTime, List<I> interpretations) {
        this.id = id;
        this.name = name;        
        this.componentsTime = new ArrayList<>();
        this.componentStats = new ArrayList<>();
        this.totalTime = totalTime;            
        
        // Format the total time (round to 2 decimal points).
        DecimalFormat df = new DecimalFormat("#0.##");
        this.totalTime = Double.valueOf(df.format(this.totalTime));

                            
        // Create a Result object for every tuple.
        this.interpretations = new ArrayList<Interpretation<I>>();
        for (I sysInterpretation : interpretations) {
            this.interpretations.add(new Interpretation<>(sysInterpretation));
        }
    }


    /**
     * Converts the response to JSON and prints it to the standard output between 
     * two lines that contain the keyword: <json>.
     * The web application will receive the response and display it properly.
     */
    public void sendToTHOR() {
        Gson gson = new Gson();
        String json = gson.toJson(this);

        // The json must be printed between this two lines.
        System.out.println("<json>");
        System.out.println(json);
        System.out.println("<json>");
        System.out.println();
    }

    /**
     * @return The id of the system.
     */
    public String getId() {
        return this.id;
    }

    /**
     * @return The name of the system.
     */
    public String getName() {
        return this.name;
    }

    /**
     * @return A list with the execution time of every component.
     */
    public List<ComponentTime> getComponentsTime() {
        return this.componentsTime;
    }

    /**
     * @return A list with the statistics of every component.
     */
    public List<ComponentStats> getComponentStats() {
        return componentStats;
    }


    /**
     * @return The total execution time of the system.
     */
    public double getTotalTime() {
        return this.totalTime;
    }

    /**
     * @return A list with the top results returned by the system.
     */
    public List<Interpretation<I>> getInterpretations() {
        return this.interpretations;
    }

}