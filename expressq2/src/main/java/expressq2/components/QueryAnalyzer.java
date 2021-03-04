package expressq2.components;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.AbstractMap.SimpleEntry;
import java.util.logging.Logger;

import shared.connectivity.thor.response.Table;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLTable;
import shared.database.model.graph.ORMNode;
import shared.database.model.graph.ORMSchemaGraph;
import expressq2.model.AnnotatedQuery;
import expressq2.model.Keyword;
import expressq2.model.Tag;
import expressq2.model.TagGrouping;
import shared.util.Pair;
import shared.util.Timer;


/** This class models the Query Analyzer Component.
 * Input: A set K = {k1, ..., kn} of keywords (query) and the ORMSchemaGraph
 * Output: List of sets of tag groupings.
 *
 * For each keyword generate a Tag. Then group all the Tags based on a
 * set of rules and output sequences of Tag Groupings. A set of Tag
 * Groupings is connected with the meaning of the keywords and the Systems
 * understanding of the query. One Query may have more than one interpretations
 * thats why the component outputs a List of sets of Tag Groupings.
 */
public class QueryAnalyzer {
    private static final Logger LOGGER = Logger.getLogger(QueryAnalyzer.class.getName());

    private SQLDatabase database;        // The database.
    private List<Keyword> keywords;      // The keywords (query).
    private ORMSchemaGraph schemaGraph;  // The ORM schema Graph.

    /** Statistics */
    double timeToAnalyzeQuery;
    public HashMap<String, SimpleEntry<Integer,Integer>> keywordNumOfMappingsMap;
    Set<String> relations; // Stores the relations where all the keywords of a query where found.
    Integer numOfIoSql;

    /**
     * The Constructor.
     *
     * @param keywords the keywords of the User's Query.
     * @param schemaGraph the schemaGraph of the database.
     */
    public QueryAnalyzer(SQLDatabase database, List<Keyword> keywords, ORMSchemaGraph schemaGraph) {
        this.database = database;
        this.keywords = keywords;
        this.schemaGraph = schemaGraph;
        this.timeToAnalyzeQuery = 0;
        this.keywordNumOfMappingsMap = new HashMap<>();
        this.relations = new HashSet<>();
    }

    /**
     * @return A list of the different interpreted Queries, annotated with schema data and metadata.
     */
    public List<AnnotatedQuery> createAnnotatedQueries() {
        // Start timer.
        Timer timer = new Timer(), t = new Timer();
        timer.start();
        numOfIoSql = 0;

        // Create the sequence List.
        List<Sequence> sequenceList = new ArrayList<>();

        // For each keyword create a ListOfTags showing the different interpretations of
        // the keyword. For each Tag create a clone of the sequence List and append the it.
        t.start();
        for (Keyword keyword: this.keywords) {
            //  Dont examine OperatorTerms.
            if (keyword.getType() == Keyword.TermType.OperatorTerm) continue;
            if (keyword.getTerm().equals(">") || keyword.getTerm().equals("<") || keyword.getTerm().equals("=")) continue;

            // Create the keyword's tags.
            t.start();
            List<Tag> keywordTags = createTags(keyword.getTerm());

            // Check if keywords Tags is empty else add the Tag to
            // the sequences in the List. if keywordTags are more
            // than one we might need to clone the sequence list
            // to create a new sequence for each interpretation (Tag).
            if (checkAndLog(keywordTags, keyword)) {
                cloneAndAddTags(sequenceList, keywordTags, keyword);
            }
        }

        // For each Sequence: Group the Tags that refer to the same object or relation.
        List<AnnotatedQuery> annotatedQueries = new ArrayList<>();
        for(Sequence sequence: sequenceList) {
            AnnotatedQuery annotatedQuery = new AnnotatedQuery(keywords);        // Create a new AnnotatedQuery
            Tag tag         = sequence.getKeywordsWithTags().get(0).getRight();  // Get the First Tag of the Sequence
            Keyword keyword = sequence.getKeywordsWithTags().get(0).getLeft();   // Get The First Keyword of the Sequence.
            TagGrouping currentGroup = new TagGrouping(tag, keyword);            // Create the First Tag Grouping containing the Tag, Keyword.

            // Add the new TagGrouping to the annotated Query.
            annotatedQuery.addTagGrouping(currentGroup);
            annotatedQuery.extractKeywordsUse(keyword, tag);

            // Loop all the Tags of the sequence (except the first one) and group them.
            for (int index = 1; index < sequence.getKeywordsWithTags().size(); index++) {
                // The tag to process along with the keyword referred to it.
                tag     = sequence.getKeywordsWithTags().get(index).getRight();
                keyword = sequence.getKeywordsWithTags().get(index).getLeft();

                // Indicates if a new group must be created.
                boolean newGroup = false;

                // Decide wether to create a new group of tags or not.
                if (!currentGroup.getLabel().equals(tag.getLabel()))
                    newGroup = true;
                else if (tag.getAttr() == null && tag.getCond() == null)
                    newGroup = true;
                else if (!this.isMultiValued(tag.getAttr(), tag.getLabel()) && currentGroup.containsTagWithEqualAttr_wCond(tag))
                    newGroup = true;

                // Create a new group for the tag if indicated by the newGroup boolean.
                if (newGroup) {
                    currentGroup = new TagGrouping(tag, keyword);
                    annotatedQuery.addTagGrouping(currentGroup);
                    annotatedQuery.extractKeywordsUse(keyword, tag);
                }
                // Else add the tag at the current Group.
                else {
                    currentGroup.addTag(tag, keyword);
                    annotatedQuery.extractKeywordsUse(keyword, tag);
                }
            }

            // Add the AnnotatedQuery to all the List.
            annotatedQueries.add(annotatedQuery);
        }

        // Stop the Timer.
        this.timeToAnalyzeQuery = timer.stop();

        // Return the groupings for each sequence.
        return annotatedQueries;
    }


    /** Create a List of Tags for the parameter keyword.  */
    public List<Tag> createTags(String keyword) {
        List<Tag> keywordTags = new ArrayList<>();   // The Tags Created for the keyword.
        Integer relationsFound = 0;                  // THe number of relations the keyword was found.

        // Loop each node in the ORMSchemaGraph.
        for (ORMNode node: this.schemaGraph.getVertexes()) {
            SQLTable relation = node.getRelation();  // Get the SQLTable from the Schema Node.

            // Call the DatabaseIndex on the connected relation.
            DatabaseIndex.numOfIoSql = 0;
            List<Tag> tags = DatabaseIndex.getKeywordsOccurrence(this.database, keyword, relation);
            this.numOfIoSql += DatabaseIndex.numOfIoSql;

            keywordTags.addAll(tags);
            // if (aggressiveTagPruning(keywordTags)) return keywordTags;  // DROPS TIME LIKE CRAZY.

            // Count the relations where a keyword was found
            if (!tags.isEmpty()) {
                relationsFound++;
                this.relations.add(relation.getName());
            }

            // Then search the component relations connected with this node.
            for (SQLTable componentRel: node.getComponentRelations()) {

                // Call the DatabaseIndex on the component relation.
                DatabaseIndex.numOfIoSql = 0;
                tags = DatabaseIndex.getKeywordsOccurrence(this.database, keyword, componentRel);
                this.numOfIoSql += DatabaseIndex.numOfIoSql;

                // if Tag is not null then swap the label of the tag. Replace the component
                // relation's name with this Relation's name.
                if (!tags.isEmpty()) {
                    for (Tag tag: tags) {
                        tag.setLabel(relation.getName());
                        tag.setComponentRelationName(componentRel.getName());
                    }
                    keywordTags.addAll(tags);
                    relationsFound++;
                    this.relations.add(relation.getName());
                }
            }
        }

        // Add to stats
        int rowsNum = 0;
        for (Tag tag: keywordTags)
            rowsNum += tag.getAppearances();
        this.keywordNumOfMappingsMap.put(keyword, new SimpleEntry<>(relationsFound, rowsNum));

        // Return the keywords after we add a Penalty to them (if needed).
        // addPenaltyToTags(keywordTags);
        // aggressiveTagPruning(keywordTags);
        return keywordTags;
    }

    /**
     * A list of Tag is Created for each keyword. In case a Tag of this list refers to
     * a Relation or Attribute in the Database then add a penalty to all other Tags. This
     * penalty will be used when scoring each Query Interpretation. We favor the Attributes
     * and Relations instead of the Values for keywords interpretation.
     *
     * @param tags The Tags created for a keyword.
     */
    private void addPenaltyToTags(List<Tag> tags) {
        boolean refersToRelOrAttrTag = false; // A Boolean indicating if there is a tag referring to Attr or Rel.

        // Loop all the Tags to find one referring to Attribute or Relation name
        for (Tag tag: tags)
            if (tag.refersToRelation() || tag.refersToAttribute()) {
                refersToRelOrAttrTag = true;
                break;
            }

        // Loop again all the tags and set a penalty to them.
        if (refersToRelOrAttrTag)
            for (Tag tag: tags)
                if (!tag.refersToRelation() && !tag.refersToAttribute())
                    tag.setPenalty(true);
    }


    /**
     * A list of Tag is Created for each keyword. In case Tags of this list refer to
     * a Relation or Attribute in the Database then prune all other Tags.  We favor the Attributes
     * and Relations instead of the Values for keywords interpretation.
     *
     * @param tags The Tags created for a keyword.
     * @return A boolean indicating if a Tag With a Relation/Attribute name was found.
     */
    private boolean aggressiveTagPruning(List<Tag> tags) {
        List<Tag> relOrAttrTags = new ArrayList<>();  // A List storing Tags that refer to Attributes or Relations.

        // Loop all the Tags.
        for (Tag tag: tags)
            // If a Tag refers to a relation or to an Attribute keep that tag instead of others.
            if (tag.refersToRelation() || tag.refersToAttribute())
                relOrAttrTags.add(tag);

         // Replace the old list with only Tags referring to Relations or Attributes
        if (!relOrAttrTags.isEmpty()) {
            tags.clear();
            tags.addAll(relOrAttrTags);
        }

        return !relOrAttrTags.isEmpty();
    }


    /**
     * Clones the sequenceList and adds to each clone one Tag from the keywordTags.
     * Each tag contains a different interpretations of the keyword. Depending on the number of Tags
     * Interpreting this keyword we will have a new sequence for each of those Tags.
     *
     * @param sequenceList the different Interpretations of the query.
     * @param keywordTags the different Interpretations of the keyword.
     * @param keyword the keyword.
     */
    private void cloneAndAddTags(List<Sequence> sequenceList, List<Tag> keywordTags, Keyword keyword) {
        // If sequenceList is empty , initialize it.
        if (sequenceList.isEmpty()) sequenceList.add(new Sequence());

        // Create a clone of the sequence List for each different interpretation of the keyword.
        int initialSize = sequenceList.size();
        int differentKeywordTags = keywordTags.size();
        cloneList(sequenceList, differentKeywordTags - 1);

        // Loop all the Lists in the sequenceList with step differentKeywordTags.
        // Each subList created by this stepping will get appended by a different
        // Tag (interpretation of the keyword).
        int innerIndex = 0, tagIndex = 0;
        for (int seqIdx = 0; seqIdx < sequenceList.size(); seqIdx++) {
            // Add a tag in the Sequence.
            sequenceList.get(seqIdx).addTag(keywordTags.get(tagIndex), keyword);

            // If we reach the index of the cloned part of the list then change
            // the tag adding to the sequences.
            if (++innerIndex == initialSize) {
                tagIndex++;
                innerIndex = 0;
            }
        }
    }


    // Clones the sequencesList cloneTimes times and appends every new clone to the sequenceList.
    private void cloneList(List<Sequence> sequenceList, int cloneTimes) {
        List<List<Sequence>> clones = new ArrayList<>();

        // First Create cloneTimes clones in the clones list.
        for (int i = 0; i < cloneTimes; i++) {
            clones.add(new ArrayList<>());
            for(Sequence sequence: sequenceList)
                clones.get(i).add(new Sequence(sequence));
        }

        // Append all the clones to the sequenceList.
        for (List<Sequence> clone: clones) {
            sequenceList.addAll(clone);
        }
    }


    // Returns true if the parameter attribute is a multi-valued attribute.
    // Multi-valued are captured by component Relations.
    public boolean isMultiValued(String attribute, String relation) {
        return this.schemaGraph.isComponentRelation(relation);
    }


    // Check if returned List of Tags for a keyword is empty and logs the waring.
    // Returns true in case keywordTags is not null.
    private boolean checkAndLog(List<Tag> keywordTags, Keyword keyword) {
        if (keywordTags == null || keywordTags.isEmpty()) {
            LOGGER.warning("Could not find keyword \"" + keyword.getTerm() + "\" in the database!");
            return false;
        }
        else
            return true;
    }


    /** Print the statistics */
    public void printStats() {
        System.out.println("QUERY ANALYZER STATS :");
        System.out.println("\tTime to analyze query: " + this.timeToAnalyzeQuery);
    }


    // public ComponentStatistics getKeywordStatistics() {
    //     List<ComponentStatistics.DescriptionValuePair> decValPair = new ArrayList<>();    // The Statistic Description / Value Pairs for Keywords
    //     decValPair.add(new ComponentStatistics.DescriptionValuePair("  ", annotatedQueries.size()));

    //     // Update Components Time and Stats
    //     componentStatistics.add( new ComponentStatistics("Query Analyzer: Keyword Mapper", decValPair) );
    // }

    public Table getStatistics() {
        String tableTittle = "Mappings";                 // The table title.
        List<String> columnTitles = new ArrayList<>();   // The column titles.
        List<Table.Row> rows = new ArrayList<>();        // The table rows.

        // Each row will contain 3 values, the keyword and the number of Tables and the number of Rows
        columnTitles.addAll(Arrays.asList("Keyword", "Tables containing Keyword", "Rows matching Keyword"));

        for (Map.Entry<String, SimpleEntry<Integer, Integer>> entry: this.keywordNumOfMappingsMap.entrySet()) {
            rows.add(new Table.Row( Arrays.asList(
                entry.getKey(), entry.getValue().getKey().toString(),
                entry.getValue().getValue().toString() ))
            );
        }

        // Return the table containing the Components Info.
        return new Table(tableTittle, columnTitles, rows);
    }

    /**
     * @return The number of relations containing all the keywords of the query.
     */
    public Integer getRelationsNumFromMappings() {
        return this.relations.size();
    }


    public Integer getNumOfIoSql() {
        return numOfIoSql;
    }

}

/**
 * This class models a sequence of tags. A sequence contains one
 * tag for each keyword of the query and as a result it
 * models one non unique way to interpret the query.
 */
class Sequence {
    private List<Pair<Keyword, Tag>> keywordsWithTags; // The tags contained in a sequence along with their keywords.

    /** Constructors */
    Sequence()               { this.keywordsWithTags = new ArrayList<>(); }
    Sequence(Sequence seq)   { this.keywordsWithTags = new ArrayList<>(seq.keywordsWithTags); }

    /**
     * @return the KeywordsWithTags list.
     */
    List<Pair<Keyword, Tag>> getKeywordsWithTags() { return this.keywordsWithTags; }


    /**
     * Adds a tag along with its keyword.
     *
     * @param tag
     * @return if the tag was inserted successfully.
     */
    boolean addTag(Tag tag, Keyword keyword) {
        return this.keywordsWithTags.add(new Pair<>(keyword,tag));
    }

    /**
     * Print the Sequence by printing the tags.
     */
    @Override
    public String toString() {
        String str = "Seq:{ ";
        for (Pair<Keyword, Tag> pair: this.keywordsWithTags)
            str += pair.getRight().toString() + ", ";
        return str.substring(0, str.length() - 2) + "}";
    }
}