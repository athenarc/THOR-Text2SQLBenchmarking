package shared.connectivity.thor.response;

import java.util.List;

/**
 * This class represents the architecture of the system as directed graph.
 * The system's components are the nodes of the graph, and the direction of the edges
 * that connect them represent the query flow of the system (the output of a component
 * serves as the input to another).
 */
public class Architecture {

    /**
     * This class represents a node in the architecture of the system.
     */
    public static class ArchitectureNode {

        private String id; // The id of the node.
        private String label; // The label of the node.

        /**
         * Constructor.
         */
        public ArchitectureNode(String id, String label) {
            this.id = id;
            this.label = label;
        }

        /**
         * @return The id of the node.
         */
        public String getId() {
            return this.id;
        }

        /**
         * @return The label of the node.
         */
        public String getLabel() {
            return this.label;
        }

    }

    /**
     * This class represents an edge (link) between two nodes in the architecture of the system.
     */
    public static class ArchitectureLink {

        private String source; // The source node.
        private String target; // The target node.
        private String label; // The label of the link.

        /**
         * Constructor.
         */
        public ArchitectureLink(String source, String target, String label) {
            this.source = source;
            this.target = target;
            this.label = label;
        }

        /**
         * @return The source node.
         */
        public String getSource() {
            return this.source;
        }

        /**
         * @return The target node.
         */
        public String getTarget() {
            return this.target;
        }

        /**
         * @return The label of the link.
         */
        public String getLabel() {
            return this.label;
        }

    }

    private List<ArchitectureNode> nodes; // The nodes of the graph.
    private List<ArchitectureLink> links; // The links between the nodes of the graph.

    /**
     * Constructor.
     */
    public Architecture(List<ArchitectureNode> nodes, List<ArchitectureLink> links) {
        this.nodes = nodes;
        this.links = links;
    }

    /**
     * @return The nodes of the architecture graph.
     */
    public List<ArchitectureNode> getNodes() {
        return this.nodes;
    }

    /**
     * @return The links between the nodes of the architecture graph.
     */
    public List<ArchitectureLink> getLinks() {
        return this.links;
    }

}