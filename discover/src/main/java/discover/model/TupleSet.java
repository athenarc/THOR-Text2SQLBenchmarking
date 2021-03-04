package discover.model;

import shared.database.model.SQLColumn;
import shared.database.model.SQLTable;
import shared.database.model.SQLTuple;
import discover.model.execution.IntermediateResultAssignment;
import shared.util.PrintingUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

// Given a keyword k and a relation (table) Ri the tuple set
// is the set of all tuples of Ri which contain the keyword k.
//
// The above definition can be generalized for a set of keywords as follows:
// Given a set of keywords K = {k1, ..., km} and a relation (table) Ri the tuple set
// is the set of all tuples of Ri which contain every keyword in K.
public class TupleSet implements JoinableExpression {

    protected SQLTable table; // The table that contains the tuples.
    protected Set<String> keywords; // The set of keywords contained in the tuple set.
    protected Set<SQLTuple> tuples; // The Tuples.

    // Set of columns of the table that contain the keywords of the tuple set.
    protected Set<SQLColumn> columnsContainingKeywords;

    protected HashMap<String, Set<SQLColumn>> keywords2columns = new HashMap<>();

    // Create an empty Tuple Set, with only the Table that it connects to.
    public TupleSet(SQLTable table) {        
        this.table = table;
        this.keywords = new HashSet<String>();
        this.tuples = new HashSet<SQLTuple>();
        this.columnsContainingKeywords = new HashSet<SQLColumn>();
    }
 
    // Create a Basic TupleSet containing only one keyword.
    public TupleSet(
        SQLTable table, String keyword, 
        Set<SQLColumn> columnsContainingKeywords, Set<SQLTuple> tuples) 
    {        
        this.table = table;
        this.keywords = new HashSet<>();
        this.keywords.add(keyword);
        this.tuples = new HashSet<>(tuples);        
        this.columnsContainingKeywords = columnsContainingKeywords;        
    }

    // Creates a new object with the given list of tuples and    
    public TupleSet(
        SQLTable table, Set<String> keywords,
        Set<SQLColumn> columnsContainingKeywords, Set<SQLTuple> tuples) 
    {
        this.table = table;
        this.keywords = new HashSet<>(keywords);        
        this.tuples = new HashSet<>(tuples);        
        this.columnsContainingKeywords = columnsContainingKeywords;        
    }

    // Copy constructor.
    public TupleSet(TupleSet src) {
        this.table = src.table;
        this.keywords = new HashSet<>(src.keywords);
        this.tuples = new HashSet<>(src.tuples);
        this.columnsContainingKeywords = new HashSet<>(src.columnsContainingKeywords);
        this.keywords2columns = new HashMap<>(src.keywords2columns);
    }


    public HashMap<String, Set<SQLColumn>> getKeywords2columns() {
        return keywords2columns;
    }

    public void setKeywords2columns(HashMap<String, Set<SQLColumn>> keywords2columns) {
        this.keywords2columns = keywords2columns;
    }

    // Getters and Setters.
    public SQLTable getTable() {
        return table;
    }

    public void setTable(SQLTable table) {
        this.table = table;
    }

    public Set<String> getKeywords() {
        return keywords;
    }

    public Set<SQLColumn> getColumnsContainingKeywords() {
        return columnsContainingKeywords;
    }

    public void setColumnsContainingKeywords(Set<SQLColumn> columnsContainingKeywords) {
        this.columnsContainingKeywords = columnsContainingKeywords;
    }

    public Set<SQLTuple> getTuples() {
        return tuples;
    }

    // Returns true if the set is empty.
    public boolean isEmpty() {
        return this.tuples.isEmpty();
    }

    // Returns the number of tuples in the set.
    public int getSize() {
        return this.tuples.size();
    }

     // Returns true if a tuple set contains a specific keyword.
     public boolean containsKeyword(String keyword) {
        return this.keywords.contains(keyword);
    }

    // Remove all tuples that contain one or more keywords in unwantedKeywords.
    public void removeTuplesContainingUnwantedKeywords(List<String> unwantedKeywords) {
        List<SQLTuple> tuplesToRemove = new ArrayList<SQLTuple>();

        // Loop all tuples of the tuple set and check if they contain any of the unwanted keywords.
        for (SQLTuple tuple : this.tuples) {
            for (String keyword : unwantedKeywords) {
                if (tuple.containsKeyword(keyword)) {
                    tuplesToRemove.add(tuple);
                }

                this.keywords2columns.remove(keyword);
            }
        }


        // Remove all the tuples with unwanted keywords.
        this.tuples.removeAll(tuplesToRemove);
    }

    // Add the tuples from the given tuple set to the tuples of the current tuple set
    // if they contain every keyword in the keywords list parameter.
    public static void addTuplesContainingEveryKeyword(
        Set<SQLTuple> acceptableTuples,
        TupleSet tupleSet,
        Set<String> keywords) 
    {
        // Loop all tuples of the tuple set and check if they contain all keywords.
        for (SQLTuple tuple : tupleSet.getTuples()) {
            // If all keywords appear in the tuple, then it's acceptable.
            if (tuple.containsAllKeywords(keywords)) {
                acceptableTuples.add(tuple);
            }
        }
    }

    // Add SQLTuples from tupleSet to the list if they contain all keywords without duplicates
    public static void addNewTuplesContainingEveryKeyword(
        Set<SQLTuple> acceptableTuples,
        TupleSet tupleSet,
        Set<String> keywords)
    {
        // Loop all tuples of TupleSet to see if
        // they have all keywords and keep them
        for (SQLTuple tuple : tupleSet.getTuples()) {            
            // If all keywords appear in the tuple and tuple does not
            // already exist in the list it is acceptable.
            if (tuple.containsAllKeywords(keywords)) {
                acceptableTuples.add(tuple);
            }
        }
    }

    // Gets the intersection of this two tuple sets
    public static TupleSet intersect(TupleSet tupleSetA, TupleSet tupleSetB) {
        // Return if Table of A is different that Table of B
        if (!tupleSetA.getTable().equals(tupleSetB.getTable())) {
            return null;
        }

        // Create a TupleSet list for the new TupleSet
        Set<SQLTuple> acceptableTuples = new HashSet<>();

        // Create a list with all the Keywords
        Set<String> allKeywords = new HashSet<String>();
        allKeywords.addAll(tupleSetA.getKeywords());
        allKeywords.addAll(tupleSetB.getKeywords());

        // Create the set with all the Columns 
        Set<SQLColumn> allColumns = new HashSet<>();
        allColumns.addAll(tupleSetA.columnsContainingKeywords);
        allColumns.addAll(tupleSetB.columnsContainingKeywords);

        // Keep the acceptable tuples of tupleSetA and tupleSetB in the same list
        TupleSet.addTuplesContainingEveryKeyword(acceptableTuples, tupleSetA, allKeywords);
        TupleSet.addNewTuplesContainingEveryKeyword(acceptableTuples, tupleSetB, allKeywords);

        // Create the new TupleSet which is the intersection of tupleSetA/B
        TupleSet ts = new TupleSet(tupleSetA.getTable(), allKeywords, allColumns, acceptableTuples);

        // Add the columns to keywords
        for (Map.Entry<String, Set<SQLColumn>> entry: tupleSetA.getKeywords2columns().entrySet())
            ts.getKeywords2columns().put(entry.getKey(), entry.getValue());

        for (Map.Entry<String, Set<SQLColumn>> entry: tupleSetB.getKeywords2columns().entrySet())
            ts.getKeywords2columns().put(entry.getKey(), entry.getValue());
        
        return ts;
    }

    // Intersects a list of tuple sets using the function
    // that intersects two tuple sets recursively.
    public static TupleSet intersect(List<TupleSet> tupleSets) {
        // The list must have at least two objects.
        if (tupleSets.size() < 2) return null;

        // Intersect the first two tuple sets.
        TupleSet intersectedTupleSet = intersect(tupleSets.get(0), tupleSets.get(1));

        // Intersect all other tuple sets with the intersectedTupleSet recursively.
        for (int i = 2; i < tupleSets.size(); i++) {
            intersectedTupleSet = intersect(intersectedTupleSet, tupleSets.get(i));
        }

        return intersectedTupleSet;
    }

    public String toAbbreviation() {
        String str = this.table.getName() + "^{";
        for (String keyword : this.keywords) {
            str += keyword + ", ";
        }

        // Remove the last ", ".
        if (!keywords.isEmpty()) {
            str = str.substring(0, str.length() - 2);
        }

        str += "}";
        return str;
    }  

    // The tupleSet cannot contain an intermediate Result as a joinable expression. It is 
    // a single joinable expression containing only one base table from the database. Return false.
    @Override
    public boolean containsIntermediateResult(IntermediateResultAssignment intermediateResult) {
        return false;
    }

    // The tupleSet is created by a base Table from the database. Return false.
    @Override
    public boolean containsOrCreatedByIntermediateResult(IntermediateResultAssignment intermediateResult) {
        return false;
    }

    // There is no intermediateResult Assignment creating a tupleSet. Return false.
    @Override
    public boolean removeIntermediateResultAssignment(IntermediateResultAssignment intermediateResult) {
        return false;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        int keywordsHash = 0;
        hash = 31 * hash + (this.table == null ? 0 : this.table.hashCode());
        for (String keyword: this.keywords) {
             keywordsHash =  keyword.hashCode();
        }
        hash = 31 * keywordsHash;
        return  hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof TupleSet)) return false;
        TupleSet set = (TupleSet) obj;

        // Two TupleSets cant contain the same keywords for the same table.
        if (!set.table.equals(this.table)) return false;
        if (set.keywords.size() != this.keywords.size()) return false;
        return set.keywords.containsAll(this.keywords);
    }

    @Override
    public String toString() {
        return this.toAbbreviation();
    }

    // Prints the list of tuples in an SQL like format.
    public void print() {
        if (this.tuples == null || this.tuples.isEmpty()) return;        
        List<SQLColumn> attributes = tuples.iterator().next().getAttributes();

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
            for (SQLTuple tuple: this.tuples) { // Loop the rows
                String value = tuple.getValues().get(index).toString();
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
        for (SQLTuple tuple: this.tuples) {
            String rowOfValues = new String();
            for (int index = 0; index < attributesNumber; index++) {
                rowOfValues += "|" + PrintingUtils.addStringWithLeadingChars(maxColumnLengthOfStrings[index],
                                        tuple.getValues().get(index).toString(), " ");
            }
            rowOfValues += "|";
            rowOfValues = rowOfValues.replace("\n", " ");
            System.out.println(rowOfValues);
        }

        // Print a separation line.
        System.out.println(separationLine);
    }

    @Override
    public Set<String> getContainedBaseTables() {
        Set<String> tables = new HashSet<>();
        tables.add(this.table.getName());
        return tables;
    }
}
