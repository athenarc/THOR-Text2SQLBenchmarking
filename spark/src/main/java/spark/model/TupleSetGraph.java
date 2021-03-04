package spark.model;

import shared.database.model.graph.SchemaGraph;
import shared.util.Graph;
import shared.util.Graph.NoLabel;

import java.util.List;
import java.util.ArrayList;

// A graph connecting tuple sets.
//
// Input: List of tuple sets.
// Output: Tuple set graph depicting relations.
//
// A node Ri^K is created for each non empty tuple set Ri^k including the free tuple sets.
// Two nodes Ri^k and Rj^M are connected if the tables Ri and Rj
// have an edge between them in the schema Graph.
public class TupleSetGraph extends Graph<TupleSet, NoLabel> {

    private List<TupleSet> tupleSets; // The output of the Post Processor component.

    public TupleSetGraph() {
        super();                
    }
   
    // Given a list of tuple sets and the schema graph with the
    // edges between the tables of the schema, this function fills the graph of tuple sets.
    public void fill(List<TupleSet> tupleSets, SchemaGraph schemaGraph) {
        if (tupleSets == null || schemaGraph == null) return;
        
        // Create all nodes in the graph.
        this.tupleSets = new ArrayList<>(tupleSets);
        for (TupleSet tupleSet: tupleSets) 
            super.addNode(tupleSet);
        
        // Create the edges in the tuple set graph.
        for (TupleSet fromSet: this.tupleSets) {
            for (TupleSet toSet: this.tupleSets) {
                // Do not allow cyclic edges.
                if (fromSet == toSet) 
                    continue;
                else if (schemaGraph.areDirConnected(fromSet.getTable(), toSet.getTable()))
                    super.addDirEdge(fromSet, toSet);
            }
        }        
    }

    // Returns true if a directed edge between two tuple sets exists.
    public boolean getDirectedConnection(TupleSet from, TupleSet to) {
        return super.areDirConnected(from, to);
    }

    // Returns true if an undirected edge between two tuple sets exists.
    public boolean getUndirectedConnection(TupleSet x, TupleSet y) {
        return super.areUnDirConnected(x, y);
    }

    
    // Returns an AdjacentTupleSets object tha contains the list of tuple sets
    // that are connected to the given tuple set ignoring direction.
    public AdjacentTupleSets getAdjacentTupleSets(TupleSet givenTupleSet) {
        List<TupleSet> adjacent = new ArrayList<TupleSet>();

        // Get the adjacent tuple sets from the graph.
        for (TupleSet tupleSet : this.tupleSets) {
            if (this.getUndirectedConnection(givenTupleSet, tupleSet)) {
                adjacent.add(tupleSet);
            }
        }

        // Check if any adjacent tuple sets were found for the given tuple set.
        if (adjacent.size() > 0) {
            return new AdjacentTupleSets(givenTupleSet, adjacent);
        }
        else {
            return null;
        }
    }

    @Override
    public String toString() {
        String str = new String();
        for (TupleSet tupleSet : this.tupleSets) {
            str += tupleSet.toAbbreviation() + " ";
        }

        str += "\n" + super.toString();
        return str;
    }

}
