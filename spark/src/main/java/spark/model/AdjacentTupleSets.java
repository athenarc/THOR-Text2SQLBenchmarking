package spark.model;

import java.util.List;
import java.util.ArrayList;

// This class contains a tuple set and a list of its adjacent tuple sets in the tuple set graph.
public class AdjacentTupleSets {

    private TupleSet tupleSet;
    private List<TupleSet> adjacentTupleSets;

    public AdjacentTupleSets() {}

    public AdjacentTupleSets(TupleSet tupleSet, List<TupleSet> adjacentTupleSets) {
        this.tupleSet = tupleSet;
        this.adjacentTupleSets = new ArrayList<TupleSet>(adjacentTupleSets);
    }

    // Getters and Setters.
    public TupleSet getTupleSet() {
        return this.tupleSet;
    }

    public List<TupleSet> getAdjacentTupleSets() {
        return this.adjacentTupleSets;
    }

    // Removes a list of tuple sets from the list of adjacent tuple sets.
    public void removeTupleSets(List<TupleSet> tupleSets) {
        this.adjacentTupleSets.removeAll(tupleSets);
    }

    @Override
    public String toString() {
        String str = new String();

        str += this.getTupleSet().toAbbreviation() + ": [";
        for (TupleSet tupleSet : this.getAdjacentTupleSets()) {
            str += tupleSet.toAbbreviation() + ", ";
        }

        // Remove the  last ", "
        if (!this.getAdjacentTupleSets().isEmpty()) {
            str = str.substring(0, str.length() - 2);
        }

        str += "]\n";

        return str;
    }

}
