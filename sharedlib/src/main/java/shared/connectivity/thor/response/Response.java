package shared.connectivity.thor.response;

import shared.connectivity.thor.response.Architecture.ArchitectureLink;
import shared.connectivity.thor.response.Architecture.ArchitectureNode;

import java.util.List;
import java.util.ArrayList;
import java.text.DecimalFormat;

import com.google.gson.Gson;

/** 
 * This class represents the response of the system to a query.
 * Contains all the information that will be displayed in the front end of the application.
 * It is parametrized with a class that implements the ResultInterface, which models a single result.
 */
public class Response<R extends ResultInterface> {

    private String id;                                 // The id of the system (the name in lowercase and without spaces).
    private String name;                               // The name of the system.
    private Architecture architecture;                 // The architecture of the system.
    private GeneralArchitecture generalArchitecture;   // The general architecture proposed by our paper. 
    private List<ComponentTime> componentsTime;        // Saves the time taken by every component of the system.
    private List<ComponentStats> componentStats;       // Saves the statistics of every component of the system
    private double totalTime;                          // The total running time of the system.
    private List<Result<R>> results;                   // The top results returned by the system.


    /** Constructor */
    public Response(String id, String name, GeneralArchitecture genArch, List<Component> components, List<R> topResults) {
        init(id, name, genArch, components, topResults);
    }

    /** Constructor without {@link GeneralArchitecture}*/
    public Response(String id, String name, List<Component> components, List<R> topResults) {
        init(id, name, null, components, topResults);
    }

    /**
     * Initializes the Response Class.
     * 
     * @param id
     * @param name
     * @param genArch
     * @param components
     * @param topResults
     */
    private void init(String id, String name,GeneralArchitecture genArch, List<Component> components, List<R> topResults) {
        this.id = id;
        this.name = name;
        this.generalArchitecture = genArch;
        this.componentsTime = new ArrayList<>();
        this.componentStats = new ArrayList<>();
        this.totalTime = 0.0;

        // The nodes and links of the architecture graph.
        List<ArchitectureNode> nodes = new ArrayList<>();
        List<ArchitectureLink> links = new ArrayList<>();

        for (Component component : components) {
            // Save the execution time of the component.
            this.componentsTime.add(new ComponentTime(component.getName(), component.getTime()));
            
            // Add the time to the total execution time.
            this.totalTime += component.getTime();

            // Save the statistics of the component.
            this.componentStats.add(new ComponentStats(component.getName(), component.getComponentInfo()));

            // Create the architecture (represented as a directed graph).
            nodes.add(new ArchitectureNode(component.getId(), component.getName()));
            List<Component> compsConnections = component.getOutGoingConnections(); // The component's outgoing connections.
            List<String> connLabels = component.getOutGoingLabels(); // The labels of the connections.

            for (int idx = 0; idx < connLabels.size(); idx++) {
                links.add(new ArchitectureLink(component.getId(), compsConnections.get(idx).getId(), connLabels.get(idx)));
            }
        }

        this.architecture = new Architecture(nodes, links);

        // Format the total time (round to 2 decimal points).
        DecimalFormat df = new DecimalFormat("#0.##");
        this.totalTime = Double.valueOf(df.format(this.totalTime));

        // ---------------------------
        // TODO UPDATE COMPONENT STATS
        // ---------------------------
                
        // // Update the percentage property of the componentsTime list.
        // for (ComponentTime componentTime : this.componentsTime) {
        //     componentTime.setPercentage(componentTime.getTime() / this.totalTime);
        // }

        // Check if general architecture's Times or outputs are Empty.
        // If so assign those lists a null object so they dont participate in the Json creation.
        if (this.generalArchitecture != null && this.generalArchitecture.areTimesEmpty())
            this.generalArchitecture.setComponentsTime(null);
        if (this.generalArchitecture != null && this.generalArchitecture.areOutputsEmpty())
            this.generalArchitecture.setComponentsOutput(null);
                            
        // Create a Result object for every tuple.
        this.results = new ArrayList<Result<R>>();
        for (R systemResult : topResults) {
            this.results.add(new Result<>(systemResult));
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
     * @return The architecture of the system.
     */
    public Architecture getArchitecture() {
        return this.architecture;
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
    public List<ComponentStats> getcomponentStats() {
        return componentStats;
    }

    /**
     * @return the generalArchitecture
     */
    public GeneralArchitecture getGeneralArchitecture() {
        return generalArchitecture;
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
    public List<Result<R>> getResults() {
        return this.results;
    }

}