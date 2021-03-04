package discoverIR.components;

import java.util.List;

import discoverIR.components.execution.engines.ExecutionEngine;
import discoverIR.components.execution.engines.NaiveExecutionEngine;
import discoverIR.components.execution.engines.SparseExecutionEngine;
import discoverIR.components.execution.engines.GlobalPipelineExecutionEngine;
import shared.database.model.SQLDatabase;
import shared.database.model.graph.SchemaGraph;
import discoverIR.model.JoiningNetworkOfTupleSets;
import discoverIR.model.Parameters;
import discoverIR.model.TupleSet;

public class ExecutionEngineInitializer {

    // Initializes an Execution Engine object depending on the parameters.
    public static ExecutionEngine initializeEngine(Parameters parameters, List<JoiningNetworkOfTupleSets> candidateNetworks,
        SchemaGraph schemaGraph, SQLDatabase database, List<TupleSet> tupleSets) {
        // Create an execution engine.
        ExecutionEngine executionEngine = null;        
        switch(parameters.executionEngineAlgorithm) {
            // Create a Naive execution engine.
            case Naive:
                executionEngine = new NaiveExecutionEngine(
                    candidateNetworks, schemaGraph, database, tupleSets,
                    parameters.maxTuples, parameters.keywords,
                    parameters.andSemantics, parameters.printResultsOrderedByTable,
                    parameters.efficientPlanGenerator
                );
                break;
            // Create a Sparse Execution Engine.
            case Sparse:
                executionEngine = new SparseExecutionEngine(
                    candidateNetworks, schemaGraph, database, tupleSets,
                    parameters.maxTuples, parameters.keywords,
                    parameters.andSemantics, parameters.printResultsOrderedByTable,
                    parameters.efficientPlanGenerator
                );
                break;
            // Create a SinglePipelined Execution Engine.
            case SinglePipelined:
                executionEngine = null;
                break;
            // Create a GlobalPipeline Execution Engine.
            case GlobalPipelined:
                executionEngine = new GlobalPipelineExecutionEngine(
                    candidateNetworks, tupleSets, schemaGraph, database,
                    parameters.maxTuples, parameters.keywords,
                    parameters.andSemantics, parameters.printResultsOrderedByTable
                );
                break;
        }

        return executionEngine;
    }

}