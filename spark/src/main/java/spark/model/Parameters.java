package spark.model;

import java.util.List;

// The Parameters class orders a number of parameters that are
// necessary for the execution of the program. These parameters 
// either stay with their default values or get a value assigned
// by the user.
public class Parameters {
    
    public boolean andSemantics; // Execute the query with and Semantics 
    public boolean printResultsOrderedByTable;  //
    public boolean efficientPlanGenerator;      //
    public int maxTuples;                       // Number of tuples that the system will return.
    public int maxNetworksSize;    
    public List<String> keywords;  // The keywords of the query.

    public Parameters(ParametersBuilder builder) {
        this.printResultsOrderedByTable = builder.printResultsOrderedByTable;
        this.maxNetworksSize = builder.maxNetworksSize;
        this.andSemantics = builder.andSemantics;
        this.maxTuples = builder.maxTuples;
        this.keywords  = builder.keywords;
    }

    //Builder Class
	public static class ParametersBuilder {

        // Required parameter,
        List<String> keywords;  // The keywords of the query.

		// Optional parameters that are initialized with default Values.
		boolean andSemantics; 
        boolean printResultsOrderedByTable;
        int maxTuples;
        int maxNetworksSize;        
		
		public ParametersBuilder(List<String> keywords){
            this.keywords = keywords;

            // Initialize with default Values.
            this.printResultsOrderedByTable = true;            
            this.maxNetworksSize = 3;
            this.andSemantics = true;
            this.maxTuples = 10;
        }
        
        public ParametersBuilder(){
            // Initialize with default Values.
            this.keywords = null;
            this.printResultsOrderedByTable = true;            
            this.maxNetworksSize = 3;
            this.andSemantics = true;
            this.maxTuples = 10;
		}

        // Set the boolean andSemantics.
		public ParametersBuilder setAndSemantics(boolean andSemantics) {
			this.andSemantics = andSemantics;
			return this;
		}

        // Set the boolean printResultsOrderedByTable.
		public ParametersBuilder setPrintResultsOrderedByTable(boolean printResultsOrderedByTable) {
			this.printResultsOrderedByTable = printResultsOrderedByTable;
			return this;
        }
        
        // Set the int maxTuples.
        public ParametersBuilder setMaxTuples(int maxTuples) {
			this.maxTuples = maxTuples;
			return this;
        }

        // Set the int maxNetworksSize.
        public ParametersBuilder setMaxNetworksSize(int maxNetworksSize) {
			this.maxNetworksSize = maxNetworksSize;
			return this;
        }
                		
		public Parameters build(){
			return new Parameters(this);
		}

	}



}