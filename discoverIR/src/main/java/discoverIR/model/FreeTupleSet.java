package discoverIR.model;

import shared.database.model.SQLTable;

import java.util.List;
import java.util.ArrayList;

// Free tuple sets are SQLTables whose tuples contain no keywords of the query.
// These tuple sets serve as a bridge between two non-free tuple sets in a joining network.
public class FreeTupleSet extends TupleSet {

    public FreeTupleSet(SQLTable table) {
        super(table);
    }

    // Returns the top tuple of the set (null for a free tuple set).
    @Override
    public OverloadedTuple getTopTuple() {
        return null;
    }

    @Override
    public int hashCode() {
        int hash = 7;        
        hash = 31 * hash + (this.table == null ? 0 : this.table.getName().hashCode());        
        // System.out.println("TupleSet_" + this.toAbbreviation() + " :" + hash);
        return  hash;
    }

    // Given a list of tables, this function returns all free tuple sets represented by those tables.
    public static List<FreeTupleSet> getFreeTupleSets(List<SQLTable> tables) {
        List<FreeTupleSet> freeTupleSets = new ArrayList<>();

        for (SQLTable table : tables) {
            freeTupleSets.add(new FreeTupleSet(table));
        }

        return freeTupleSets;
    }

}
