package discover.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.database.model.SQLTable;
import discover.model.TupleSet;

// A class to keep the tuple sets of a specific table.
public class TableTupleSets {

    private SQLTable table; // The table containing the tuples sets.
    private List<TupleSet> tupleSets; // All tuple sets of the specific table.

    public TableTupleSets(SQLTable table) {
        this.table = table;
        this.tupleSets = new ArrayList<TupleSet>();
    }

    public TableTupleSets(TupleSet tupleSet) {
        this.table = tupleSet.getTable();
        this.tupleSets = new ArrayList<TupleSet>();
        this.tupleSets.add(tupleSet);
    }

    public TableTupleSets(SQLTable table, List<TupleSet> tupleSets) {
        this.table = table;
        this.tupleSets = tupleSets;
    }

    // Getters and Setters.
    public SQLTable getTable() {
        return this.table;
    }

    // Return the tupleSets
    public List<TupleSet> getTupleSets() {
        return tupleSets;
    }

    // Get the basic tuple set of a specific keyword.
    public TupleSet getBasicTupleSetOfKeyword(String keyword) {
        for (TupleSet tupleSet : this.tupleSets) {
            for (String tupleSetKeyword : tupleSet.getKeywords()) {
                if (tupleSetKeyword.equals(keyword)) {
                    return tupleSet;
                }
            }
        }

        return null;
    }

    // Get all the keywords that correspond to the current table's tuple sets.
    // (Meaning the keywords that were found in any of the table's tuples.)
    public Set<String> getKeywords() {
        Set<String> keywords = new HashSet<>();

        for (TupleSet tupleSet : this.tupleSets) {  // TODO HANDLE DUPLICATES
            for (String tupleSetKeyword : tupleSet.getKeywords()) {
                keywords.add(tupleSetKeyword);
            }
        }

        return keywords;
    }

    // Adds a tuple set to the current table's list of  tuple sets.
    public void addTupleSet(TupleSet tupleSet) {
        if (!this.table.equals(tupleSet.getTable())) {
            return;
        }

        this.tupleSets.add(tupleSet);
    }

}
