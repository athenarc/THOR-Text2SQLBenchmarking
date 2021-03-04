package spark.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Comparator;
import java.util.Collections;

// This class models a Strata (Spark Paper page 7).
// The Strata stores tuples of a Tuple Set with the same signature.
// In simple words all tuples contained in a Strata have the same
// number of occurrences of the same keywords.
// The Strata objects only differ the TupleSet objects in one extra field
// with is the signature field.
// So it is profound why it extends the TupleSet class.
public class Stratum extends TupleSet {

    // A comparator used to sort a collection of strata based on their signatures' watf value.
    public static class ScoreComparator implements Comparator<Stratum> {
        @Override
        public int compare(Stratum a, Stratum b) {
            return b.getSignature().getWatf().compareTo(a.getSignature().getWatf());
        }
    }

    private Signature signature; // The stratum's signature.

    private int index; // The index of the stratum in the strata list of its tuple set.
    private double watfOfTopTuple; // The watf value of the stratum's top tuple.

    public Stratum(Signature signature, TupleSet tupleSet, List<OverloadedTuple> tuples) {
        super(tupleSet, tuples);
        this.signature = signature;
    }

    // Getters and Setters.
    public Signature getSignature() {
        return this.signature;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public double getWatfOfTopTuple() {
        return watfOfTopTuple;
    }

    public void setWatfOfTopTuple(double watfOfTopTuple) {
        this.watfOfTopTuple = watfOfTopTuple;
    }

    // Returns the tuples of the stratum.
    public List<OverloadedTuple> getTuples() {
        return super.tuples;
    }

    // Adds a tuple to the stratum's list of tuples.
    public void addTuple(OverloadedTuple tuple) {
        super.tuples.add(tuple);
    }

    // Given a map of signatures and their corresponding lists of tuples,
    // this function returns a list of stratum objects equivalent to the map.
    public static List<Stratum> mapToList(Map<Signature, List<OverloadedTuple>> strataMap, TupleSet tupleSet,
                JoiningNetworkOfTupleSets network) {
        List<Stratum> strata = new ArrayList<>();

        // Create a stratum out of every map entry.
        for (Map.Entry<Signature, List<OverloadedTuple>> entry : strataMap.entrySet() ) {
            Stratum stratum = new Stratum(entry.getKey(), tupleSet, entry.getValue());
            stratum.getSignature().computeWatf(network);
            strata.add(stratum);
        }

        // Sort the strata based on their signatures' watf value.
        Collections.sort(strata, new Stratum.ScoreComparator());

        return strata;
    }

    @Override
    public int hashCode() {
        return this.signature.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null) return false;
        if (!(obj instanceof Stratum)) return false;
        Stratum st = (Stratum) obj;

        // Two strata are equal if the have the same signature.
        return this.signature.equals(st.signature);
    }

    @Override
    public String toString() {
        String str = new String();
        str += "Stratum with signature : " + this.signature.toString() + "\n";
        for (OverloadedTuple tuple: this.tuples) {
            str += tuple.toString() + "\n";
        }

        return str;
    }

}
