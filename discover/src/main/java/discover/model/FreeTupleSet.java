package discover.model;

import java.util.List;
import java.util.ArrayList;

import shared.database.model.SQLTable;

// Free tuple sets are SQLTables whose tuples contain no keywords of the query.
// These tuple sets serve as a bridge between two non-free tuple sets in a joining network.
public class FreeTupleSet extends TupleSet {

    public FreeTupleSet(SQLTable table) {
        super(table);
    }

    @Override
    public void removeTuplesContainingUnwantedKeywords(List<String> unwantedKeywords) {
        return;
    }

    @Override
    public boolean containsKeyword(String keyword) {
        return false;
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
