package discover.components;

import shared.connectivity.thor.response.Table;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import discover.DiscoverApplication;
import discover.model.TableTupleSets;
import discover.model.TupleSet;
import shared.util.Timer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Input: Basic tuple sets and a set of keywords {k1, ..., km}
// Output: TupleSets Ri^K containing tuples with keywords in all 
// the subsets Ksub of K={k1, ..., km} and no other keywords
// except from K - Ksub for a relation i.
public class TupleSetPostProcessor {

    private List<String> keywords; // All keywords of the query.
    private List<TupleSet> basicTupleSets; // All basic tuple sets of the single keywords.
    private List<TableTupleSets> tableBasicTupleSets; // All basic tuple sets sorted by the table they belong to.

    // Statistics
    private double timeGeneratingTupleSets;

    // The output of the TupleSetPostProcessor as described above.
    // Contains the TupleSets Ri^K of the keyword subsets K.
    List<TupleSet> tupleSetCombinations;

    public TupleSetPostProcessor(List<String> keywords, List<TupleSet> basicTupleSets) {
        this.keywords = keywords;
        this.basicTupleSets = basicTupleSets;
        this.tableBasicTupleSets = new ArrayList<TableTupleSets>();
        this.tupleSetCombinations = new ArrayList<TupleSet>();
    }

    // Getters and Setters.
    public List<TupleSet> getKeywordSubsetTupleSets() {
        return this.tupleSetCombinations;
    }

    // The main function of the TupleSetPostProcessor component.
    // Generates a tuple set for every subset of the keywords.
    public List<TupleSet> generateKeywordSubsetsTupleSets() {
        // Start the timer.
        Timer timer = new Timer();
        timer.start();

        // Order the basic tuple sets by the table they belong to.
        this.orderBasicTupleSetsByTable();

        // Create the tuple sets Ri^K as described above.
        for (TableTupleSets currentTableTupleSets : this.tableBasicTupleSets) {
            // Get all TupleSets containing keyword subsets contained in current table.
            List<TupleSet> tableSubsets = this.createTableSubsets(currentTableTupleSets);

            // Add them to the post processor component's output list.            
            this.tupleSetCombinations.addAll(tableSubsets);
        }

        // Stop the timer.
        this.timeGeneratingTupleSets = timer.stop();

        return this.tupleSetCombinations;
    }

    // Given the basic tuple sets of a table, this function creates all possible subsets
    // of keywords from the list of keywords contained in the table's basic tuple sets.
    // Then, for every subset of keywords it creates the tuple set from the given table
    // that contains all keywords of the subset.
    public List<TupleSet> createTableSubsets(TableTupleSets tableBasicTupleSets) {        
        // List of tuple sets for every subset of keywords.
        List<TupleSet> subsetsTupleSets = new ArrayList<TupleSet>();

        // Get all the keywords contained in the current table's basic tuple sets.
        List<String> tableKeywords = new ArrayList<>(tableBasicTupleSets.getKeywords());        
        int keywordsNumber = tableKeywords.size();
        int numOfSubsets = 1 << keywordsNumber;        

        // Generate all possible subsets of the keywords and save their corresponding tuple sets.
        for (int subsetNum = 1; subsetNum < numOfSubsets; subsetNum++) {
            // The tuple sets of the single keywords that make up a set of keywords.
            List<TupleSet> tupleSetsCombination = new ArrayList<TupleSet>();            

            // If not create it with basic tuple sets of single keywords.
            for (int pos = 0; pos < keywordsNumber; pos++) {
                if ((subsetNum & (1 << pos)) > 0) {
                    TupleSet keywordTupleSet = tableBasicTupleSets.getBasicTupleSetOfKeyword(
                        tableKeywords.get(pos)
                    );
                    if (keywordTupleSet != null) {                        
                        tupleSetsCombination.add(keywordTupleSet);
                    }
                }
            }

            // Create the tuple set of the current subset of keywords.
            TupleSet subset = this.createSubset(tupleSetsCombination, this.keywords);
            if (!subset.isEmpty()) {
                subsetsTupleSets.add(subset);
            }
        }

        return subsetsTupleSets;
    }

    // Given a list of basic tuple sets create a new tuple set that contains all keywords
    // of the given tuple sets, but no other keyword from the allKeywords list.
    //
    // Specifically, for a subset K of {k1, ..., km} create a tuple set where every tuple
    // in the set contains all keywords of K, but no keyword of {k1, ..., km} - K.
    public TupleSet createSubset(List<TupleSet> tupleSetsCombination, List<String> allKeywords) {
        // Get the first tuple set.
        TupleSet intersectedSet = new TupleSet(tupleSetsCombination.get(0));

        // Intersect it with the remaining tuple sets recursively.
        for (int i = 1; i < tupleSetsCombination.size(); i++) {
            intersectedSet = TupleSet.intersect(intersectedSet, tupleSetsCombination.get(i));
        }        
        
        // Create a list with all keywords that should not be contained in the tuple set.
        // This list is the set {k1, ..., km} - K.
        List<String> unwantedKeywords = new ArrayList<String>();
        for (String keyword : allKeywords)
            if (!intersectedSet.getKeywords().contains(keyword))
                unwantedKeywords.add(keyword);                

        // Remove all tuples with unwanted keywords.
        intersectedSet.removeTuplesContainingUnwantedKeywords(unwantedKeywords);

        return intersectedSet;
    }

    // Returns the index of a table in the tableBasicTupleSets list.
    // Returns -1 if it was not found.
    public int getIndexOfTable(SQLTable table) {
        for (int index = 0; index < this.tableBasicTupleSets.size(); index++) {
            if (this.tableBasicTupleSets.get(index).getTable().equals(table)) {
                return index;
            }
        }

        return -1;
    }

    // Orders the basic tuple sets by the table they belong to.
    // A table may have multiple basic tuple sets depending on the keywords that its tuples contain.
    // Stores the result in the tableBasicTupleSets list.
    public void orderBasicTupleSetsByTable() {
        if (!this.tableBasicTupleSets.isEmpty()) {
            return;
        }

        // Add every basic tuple set to the tuple set list of the table they belong to.
        for (TupleSet tupleSet : this.basicTupleSets) {
            int index = getIndexOfTable(tupleSet.getTable());

            if (index != -1) {
                this.tableBasicTupleSets.get(index).addTupleSet(tupleSet);
            }
            else {
                this.tableBasicTupleSets.add(new TableTupleSets(tupleSet));
            }
        }
    }

    // Print the statistics
    public void printStats() {
        System.out.println("TUPLE SETS PRE PROCESSOR STATS :");
        System.out.println("\tTime to generate TupleSets Combos: " + this.timeGeneratingTupleSets);
    }

    // Fill the Statistics we want to display on Thor
    public Table getStatistics() {                
        List<Table.Row> rows = new ArrayList<>();  // The table rows.
                        
        rows.add(new Table.Row( Arrays.asList(
            "Num of Basic TupleSets",
            Integer.toString(this.basicTupleSets.size())
        )));

        rows.add(new Table.Row( Arrays.asList(
            "Num of Combinations (using the above TupleSets)",
            Integer.toString(this.tupleSetCombinations.size())
        )));
                
        // Return the table containing the Components Info.
        return new Table(rows);
    }

}






//  +==========================+
//  | TupleSet HashTable Class |
//  +==========================+

class TupleSetsHashTable {
    // A list of HashMaps ordered in the cardinality of the sets they include.
    // The BitSet represents what keywords each tupleSet contains (with 1) and not contain (with 0).
    List<Map<BitSet,TupleSet>> listOfMaps; 

    // Public Constructor
    TupleSetsHashTable (int maxSetCardinality) {
        this.listOfMaps = new ArrayList<Map<BitSet,TupleSet>>(maxSetCardinality);
        for (int index=0; index < maxSetCardinality; index++) {
            this.listOfMaps.add(new HashMap<BitSet,TupleSet>());
        }
    }

    public static BitSet convert(int value) {
        BitSet bits = new BitSet();
        int index = 0;
        while (value != 0L) {
            if (value % 2L != 0) {
                bits.set(index);
            }
            index++;
            value = value >>> 1;
        }
        return bits;
    }

    // Add a new tuple set
    void add(BitSet bitset, TupleSet tupleSet) {
        // Determine the Hash Map that this tuple set will
        // be added to depending on his bitset sum of 1.
        // Which means on the number of tupleSets contained
        // in the TupleSet.
        int index = bitset.cardinality() - 1;
        this.listOfMaps.get(index).put(bitset, tupleSet);
    }

    // Get the TupleSet mapped to bitset
    TupleSet getTupleSetWithBitSet(BitSet bitset){
        // The hash map that this biteSet's tuple 
        // is, can be located from the bitSets sum of 1.
        Map<BitSet,TupleSet> map = this.listOfMaps.get(bitset.cardinality() - 1);
        return map.get(bitset);

    }

    // Get a pair of TupleSets that create the give bitSet of keywords
    List<TupleSet> getPairCreatingTupleSetWithBitSet(BitSet bitset) {
        // Dont search for TupleSets containing less than two keywords
        if (bitset.cardinality() < 2) {
            return null;
        }

        // Find the first occurrence of a bit set to 1
        int index = bitset.nextSetBit(0);

        // Candidate one :
        // Create a bit set with the same size but with only that bit set to 1
        BitSet candidateOne = new BitSet(bitset.length());
        candidateOne.set(index);

        // Candidate two :
        BitSet candidateTwo = (BitSet) bitset.clone();
        candidateTwo.clear(index);

        // Search for both candidates in the hash map.
        // They should be there because of the way that Tuple
        // sets are created from the callee function.
        TupleSet tupleSetOne = this.getTupleSetWithBitSet(candidateOne);
        TupleSet tupleSetTwo = this.getTupleSetWithBitSet(candidateTwo);

        // Return the candidates or null if a candidate was not found.
        if (tupleSetOne == null || tupleSetTwo == null) {
            return null;
        }
        List<TupleSet> resultList = new ArrayList<TupleSet>();
        resultList.add(tupleSetOne);
        resultList.add(tupleSetTwo);

        if (DiscoverApplication.DEBUG_PRINTS) {
            System.out.println("Candidate TupleSets for creating a set");
            for (TupleSet set: resultList) {
                System.out.println(set.toAbbreviation());
                for (SQLTuple tup: set.getTuples()) {
                    System.out.println("\t" + tup);
                }
            }
        }

        return resultList;
    }
        
}