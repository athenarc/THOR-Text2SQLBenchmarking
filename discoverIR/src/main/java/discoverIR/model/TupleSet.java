package discoverIR.model;

import shared.database.model.SQLColumn;
import shared.database.model.SQLDoubleValue;
import shared.database.model.SQLTable;
import shared.database.model.SQLType;
import shared.database.model.SQLValue;
import shared.util.PrintingUtils;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collections;

// Associates a table with its tuple set.
// Given a query Q and a table (relation) Ri the tuple set of Ri
// is the set of all tuples of Ri for which the RDBMS returned a non-zero score.
// The tuples in the tuple set are sorted in descending order based on their score.
public class TupleSet {

    protected SQLTable table;        // The table that contains the tuples.
    protected Set<String> keywords;  // The set of keywords contained in the tuple set.
    protected List<OverloadedTuple> tuples; // The sorted tuples.

    // Set of columns of the table that contain the keywords of the tuple set.
    protected Set<SQLColumn> columnsContainingKeywords;

    // Maps a keyword with the percentage of rows (of the table) that contain it.
    private Map<String, Double> keywordSelectivities;

    public TupleSet(SQLTable table) {
        this.table = table;
        this.keywords = new HashSet<String>();
        this.tuples = new ArrayList<OverloadedTuple>();
        this.columnsContainingKeywords = new HashSet<SQLColumn>();
        this.keywordSelectivities = new HashMap<String, Double>();
    }

    // Creates a new object that contains only the given tuple.
    public TupleSet(SQLTable table, Set<SQLColumn> columnsContainingKeywords, OverloadedTuple tuple) {
        this.table = table;
        this.keywords = new HashSet<String>();
        this.tuples = new ArrayList<OverloadedTuple>();
        this.tuples.add(tuple);
        this.columnsContainingKeywords = columnsContainingKeywords;
        this.keywordSelectivities = new HashMap<String, Double>();
    }

    // Creates a new object with the given list of tuples and
    // sorts them in descending order based on their score.
    public TupleSet(SQLTable table, Set<SQLColumn> columnsContainingKeywords, Set<OverloadedTuple> tuples) {
        this.table = table;
        this.keywords = new HashSet<String>();
        this.tuples = new ArrayList<OverloadedTuple>();
        this.tuples.addAll(tuples);
        Collections.<OverloadedTuple>sort(this.tuples, new OverloadedTuple.ScoreComparator());
        this.columnsContainingKeywords = columnsContainingKeywords;
        this.keywordSelectivities = new HashMap<String, Double>();
    }

    // Creates a new object with the given list of tuples and
    // sorts them in descending order based on their score.
    public TupleSet(SQLTable table, Set<SQLColumn> columnsContainingKeywords, List<OverloadedTuple> tuples) {
        this.table = table;
        this.keywords = new HashSet<String>();
        this.tuples = new ArrayList<OverloadedTuple>();
        this.tuples.addAll(tuples);
        Collections.<OverloadedTuple>sort(this.tuples, new OverloadedTuple.ScoreComparator());
        this.columnsContainingKeywords = columnsContainingKeywords;
        this.keywordSelectivities = new HashMap<String, Double>();
    }

    // Copy constructor.
    public TupleSet(TupleSet src) {
        this.table = src.table;
        this.keywords = new HashSet<String>(src.keywords);
        this.tuples = new ArrayList<>(src.tuples);
        this.columnsContainingKeywords = new HashSet<SQLColumn>(src.columnsContainingKeywords);
        copyMap(src.keywordSelectivities, this.keywordSelectivities);
    }

    // Copy constructor with a new list of tuples.
    public TupleSet(TupleSet tupleSet, List<OverloadedTuple> newTuples) {
        this.table = tupleSet.table;
        this.keywords = new HashSet<String>(tupleSet.keywords);
        this.tuples = new ArrayList<>(newTuples);
        this.columnsContainingKeywords = new HashSet<SQLColumn>();
        this.columnsContainingKeywords.addAll(tupleSet.columnsContainingKeywords);
    }

    // Getters and Setters.
    public SQLTable getTable() {
        return this.table;
    }

    public void setTable(SQLTable table) {
        this.table = table;
    }

    public Set<String> getKeywords() {
        return this.keywords;
    }

    public List<OverloadedTuple> getTuples() {
        return this.tuples;
    }

    public Set<SQLColumn> getColumnsContainingKeywords() {
        return this.columnsContainingKeywords;
    }

    public void setColumnsContainingKeywords(Set<SQLColumn> columnsContainingKeywords) {
        this.columnsContainingKeywords = columnsContainingKeywords;
    }

    public Map<String, Double> getKeywordSelectivities() {
        return this.keywordSelectivities;
    }

    // Returns the selectivity of a keyword.
    public Double getKeywordSelectivity(String keyword) {
        return this.keywordSelectivities.get(keyword);
    }

    // Returns true if the set is empty.
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    // Returns the number of tuples in the set.
    public int getSize() {
        return this.tuples.size();
    }

    // Returns the top tuple of the set (the one with the maximum score).
    // Returns null if tuple set is empty.
    public OverloadedTuple getTopTuple() {
        if (!this.tuples.isEmpty()) {
            return this.tuples.get(0);
        }
        else {
            return null;
        }
    }

    // Returns the tuple of the set whose position is specified by the given index value.
    // Returns null if the index is bigger than the number of tuples in the set.
    public OverloadedTuple getTupleByIndex(int index) {
        if (index < this.tuples.size()) {
            return this.tuples.get(index);
        }
        else {
            return null;
        }
    }

    // Returns the subset of tuples from the beginning up to the given index value.
    // Returns the whole set if the index is bigger than the number of tuples in the set.
    public List<OverloadedTuple> getTuplesUpToIndex(int index) {
        if (index < this.tuples.size()) {
            return this.tuples.subList(0, index);
        }
        else {
            return this.tuples;
        }
    }

    // Finds and saves the keywords from the given list that the tuple set contains.
    public void setKeywords(List<String> keywords) {
        for (OverloadedTuple tuple : this.tuples) {
            for (String keyword : keywords) {
                if (tuple.containsKeyword(keyword)) {
                    this.keywords.add(keyword);
                }
            }
        }
    }


    // Returns true if a tuple set contains a specific keyword.
    public boolean containsKeyword(String keyword) {
        return this.keywords.contains(keyword);
    }

    public void computeKeywordStatistics(List<String> keywords) {
        // Get the columns of the table that are indexed.
        List<SQLColumn> indexedColumns = this.table.getIndexedColumns();

        for (OverloadedTuple tuple : this.tuples) {
            for (String keyword : keywords) {
                boolean tupleContainsKeyword = false; // Indicates whether the tuple contains the keyword at least once.

                for (SQLColumn column : indexedColumns) {
                    // Check if the current keyword is contained in the value of the current column.
                    SQLValue value = tuple.getValueOfColumn(column);
                    if ((value != null) && (value.contains(keyword))) {
                        tupleContainsKeyword = true;
                        tuple.incrementKeywordFrequency(keyword);
                    }
                }

                // Initially the map stores the number of rows that every keywords appears in.
                if (tupleContainsKeyword == true) {
                    this.keywordSelectivities.put(keyword, this.keywordSelectivities.getOrDefault(keyword, 0.0) + 1);
                }
            }
        }

        // Divide the selectivities of every keyword by the size of the table.
        for (Map.Entry<String, Double> entry : this.keywordSelectivities.entrySet()) {
            entry.setValue(entry.getValue() / this.table.getRowsNum());
        }
    }

    // Performs a deep copy operation between to map objects.
    // The first argument is the source, and the second is the destination.
    private void copyMap(Map<String, Double> src, Map<String, Double> dest) {
        for (Map.Entry<String, Double> entry : src.entrySet()) {
            dest.put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public int hashCode() {
        int hash = 7;
        int tuplesHash = 1;
        hash = 31 * hash + (this.table == null ? 0 : this.table.getName().hashCode());
        for (OverloadedTuple tuple: this.tuples) {
             tuplesHash =  tuple.hashCode();
        }
        hash += 31 * tuplesHash;
        // System.out.println("TupleSet_" + this.toAbbreviation() + " :" + hash);
        return  hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof TupleSet)) return false;

        TupleSet set = (TupleSet) obj;
        return set.table.equals(this.table) && set.keywords.containsAll(this.keywords);
    }

    public String toAbbreviation() {
        String str = this.table.getName() + "^{";
        if (!(this instanceof FreeTupleSet)) {
            str += "Q";
        }
        str += "}";

        return str;
    }

    // Prints the top tuples of the set up to the given index value.
    public void printTuplesUpToIndex(int index) {
        TupleSet temp = new TupleSet(this.getTable(), this.getColumnsContainingKeywords(), this.getTuplesUpToIndex(index));
        temp.print(true);
    }

    // Prints the list of tuples in an SQL like format.
    public void print(Boolean printScore) {
        if (this.tuples == null || this.tuples.isEmpty()) return;

        List<SQLColumn> attributes = tuples.get(0).getAttributes();

        // Add the score attribute.
        if (printScore) {
            SQLType columnType = new SQLType("double", 0);
            SQLColumn scoreColumn = new SQLColumn(null, "score", columnType, "");
            attributes.add(scoreColumn);

            // Add the value of the score column.
            for(OverloadedTuple tuple: this.tuples) {
                SQLValue scoreValue = new SQLDoubleValue(tuple.getScore());
                tuple.addValue(scoreValue);
            }
        }

        // Initialize number of attributes.
        int attributesNumber = attributes.size();

        // An array storing the max length of Strings per column.
        // The Strings per column are tuple Values and attributes.
        int[] maxColumnLengthOfStrings = new int[attributesNumber];

        // Loop all the attributes and fill the max length array
        for (int index = 0; index < attributesNumber; index++) {
            // Initialize the array with the attributes name length.
            maxColumnLengthOfStrings[index] = attributes.get(index).getName().length();

            // Loop the values and find the longest value toString().
            for (int rowIndex = 0; rowIndex < this.tuples.size(); rowIndex++) { // Loop the rows
                String value = this.tuples.get(rowIndex).getValues().get(index).toString();
                if (value.length() > maxColumnLengthOfStrings[index]){
                    maxColumnLengthOfStrings[index] = value.length();
                }
            }
        }

        // The list of attributes to String format.
        String attributesList = new String();

        // A line used to separate the attributes from the data.
        String separationLine = new String();

        // Create the separation line and the attributes line.
        for (int index = 0; index < attributesNumber; index++) {
            attributesList += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                attributes.get(index).getName(), " ");
            separationLine += "+" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index], "", "-");
        }
        attributesList += "|";  // Add the last "|".
        separationLine += "+"; // Add the last "+".

        // Print the attributes between separation lines.
        System.out.println(separationLine);
        System.out.println(attributesList);
        System.out.println(separationLine);

        // Print all the rows of Tuple Values.
        for (OverloadedTuple tuple: this.tuples) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                        tuple.getValues().get(index).toString(), " ");
            }
            rowOfValues += "|";
            rowOfValues += " " + tuple.getKeywordFrequencies();
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);

        if (printScore) {
            // remove score from attribute list.
            attributes.remove(attributes.size() - 1);

            // Remove the value of the score column.
            for(OverloadedTuple tuple: this.tuples) {
                tuple.removeLastValue();
            }
        }
    }

    @Override
    public String toString() {
        String str = this.toAbbreviation() + "\n";
        if (!(this instanceof FreeTupleSet)) {
            for (int i = 0; i < this.tuples.size() && i < 20; i++ )            
                str += this.tuples.get(i) + "\n";            
        }

        return str;
    }

}
