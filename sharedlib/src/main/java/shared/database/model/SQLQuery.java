package shared.database.model;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import shared.util.Pair;
import shared.util.PrintingUtils;

public class SQLQuery {

    private List<String> selectElements; // The columns selected to participate in the Query.
    private List<String> fromElements; // The Tables selected to participate in the Query.
    private List<String> joinEquations; // The equations used to join the above from Elements.
    private List<String> inListConstraints; // In List Assignments used in the specific query.
    private List<String> valueConstraints; // Value specific constraints.
    private List<String> groupByElements; // The columns contained in the groupBy part.

    private List<Pair<String, String>> tablesWithAliases; // Contains the table names along with their aliases

    /** Constructor */
    public SQLQuery() {
        this.selectElements = new ArrayList<>();
        this.fromElements = new ArrayList<>();
        this.joinEquations = new ArrayList<>();
        this.inListConstraints = new ArrayList<>();
        this.valueConstraints = new ArrayList<>();
        this.groupByElements = new ArrayList<>();
        this.tablesWithAliases = new ArrayList<>();
    }
 

    /**     
     * Adds a Aggregation Function on a column to the selectedElements List.
     * 
     * @param aggregationTerm the Aggregation Function applied on a column.
     * @param column the column.
     * @return A boolean indicating if the insertion succeeded.
     */
    public boolean addAggregation(String aggregationTerm, String column) {
        return this.selectElements.add(aggregationTerm + "(" + column + ")");
    }    

    /**
     * Create an equality condition by merging the two parameter terms in the string : 
     *          leftTerm = rightTerm
     * 
     * @param leftTerm the left term of the Equality Condition.
     * @param rightTerm the right term of the Equality Condition.
     * @return A boolean indicating if the insertion succeeded.
     */
    public boolean addJoinCondition(String leftTerm, String rightTerm) {
        return this.joinEquations.add(leftTerm + " = " + rightTerm);
    } 

    /**
     * Create an containing condition by merging the two parameter terms in the string : 
     *          match(column) against()
     * 
     * @param value the values to search for.
     * @param column the column to search for the parameter value.
     * @return A boolean indicating if the insertion succeeded.
     */
    public boolean addMatchAgainstConstraint(String column, String value) {
        return this.valueConstraints.add(String.format(SQLQueries.SQL_MATCH_AGAINST_STMT, column, value));
    } 

    /**
     * Adds a table name and its respective alias for the SQLQuery.
     */
    public void addTableWithAlias(String tableName, String alias) {
        this.tablesWithAliases.add(new Pair<>(tableName, alias));

        // Add it also in the from elements variable
        this.fromElements.add(tableName + SQLQueries.ALIAS_SPECIFIER + alias);
    }

    /**
     * Returns the Tables located in the from Part of the SQLQuery 
     */
    public Set<String> getTables() {
        Set<String> tables = new HashSet<>();
        for (Pair<String, String> table: this.tablesWithAliases) {
            String[] tokens = table.getLeft().split("_");
            tables.add( 
                tokens[tokens.length - 1]
            );
        }
        return tables;
    }

    /**
     * Returns a SQL Select Query without and IN List Constraints or temp tables in the 
     * From Clause.
     */
    public String toPrettyQuery() {
        String query = new String();                               

        // Fill in the Select and From clause of the query
        query += String.format(
            SQLQueries.SQL_SELECT_QUERY, 
            PrintingUtils.separateWithDelimiter(this.selectElements, ", ") + "\n",     // Select Part
            PrintingUtils.separateWithDelimiter(this.getFromElementsNoTemps(), ", ")
        );
        
        // Create a list containing all the where conditions.
        List<String> whereElements = new ArrayList<>();
        whereElements.addAll(this.joinEquations);
        whereElements.addAll(this.valueConstraints);
        
        // If there are in list constraints carefully add them by removies the ids
        for (String inListConst: this.inListConstraints) {
            whereElements.add(inListConst.substring(0, inListConst.indexOf("IN (") + 4) + " ... )");
        }
        
        // Add where clause if it exists
        if (!whereElements.isEmpty()) {
            query += "\n" + String.format(
                SQLQueries.SQL_WHERE_CLAUSE,
                PrintingUtils.separateWithDelimiter(whereElements, SQLQueries.ADD_WHERE_STMT)
            );
        }

        // Add GroupBy clause if it exists
        if (!this.groupByElements.isEmpty()) {
            query += String.format(
                SQLQueries.SQL_GROUPBY_CLAUSE,
                PrintingUtils.separateWithDelimiter(this.groupByElements, ", ")
            );
        }

        return query;
    }


    /**
     * Returns the SQL Select Query in string format.
     */
    public String toSelectQuery() {        
        String query = new String();                        

        // Fill in the Select and From clause of the query
        query += String.format(
            SQLQueries.SQL_SELECT_QUERY, 
            PrintingUtils.separateWithDelimiter(this.selectElements, ", ") + "\n",   // Select Part
            PrintingUtils.separateWithDelimiter(this.fromElements, ", ")
        );
        
        // Create a list containing all the where conditions.
        List<String> whereElements = new ArrayList<>();
        whereElements.addAll(this.joinEquations);        
        whereElements.addAll(this.valueConstraints);        
        whereElements.addAll(this.inListConstraints);

        // Add where clause if it exists
        if (!whereElements.isEmpty()) {
            query += "\n" + String.format(
                SQLQueries.SQL_WHERE_CLAUSE,
                PrintingUtils.separateWithDelimiter(whereElements, SQLQueries.ADD_WHERE_STMT)
            );
        }

        // Add GroupBy clause if it exists
        if (!this.groupByElements.isEmpty()) {
            query += String.format(
                SQLQueries.SQL_GROUPBY_CLAUSE,
                PrintingUtils.separateWithDelimiter(this.groupByElements, ", ")
            );
        }

        // Add a limit to the query 
        query += " " + SQLQueries.LIMIT_STATEMENT_2K;

        return query;
    }

    /**
     * Returns the from Elements of the Query but removes the temp
     * tables overloaded Names and keeps the Table Names as it is on 
     * the SQLDatabase
     */
    public List<String> getFromElementsNoTemps() {
        List<String> processedFromElements = new ArrayList<>();
        for (String fromElement: this.fromElements) {
            String[] tokens = fromElement.split("_");
            processedFromElements.add( 
                tokens[tokens.length - 1]
            );
        }
        return processedFromElements;
    }

    /**
     * @return the fromElements
     */
    public List<String> getFromElements() {
        return fromElements;
    }

    /**
     * @return the selectElements
     */
    public List<String> getSelectElements() {
        return selectElements;
    }

   /**
    * @return the inListConstraints
    */
   public List<String> getInListConstraints() {
       return inListConstraints;
   }

   /**
    * @return the joinEquations
    */
   public List<String> getJoinEquations() {
       return joinEquations;
   }

   /**
    * @return the valueConstraints
    */
   public List<String> getValueConstraints() {
       return valueConstraints;
   }

    /**
     * @return the groupByElements
     */
    public List<String> getGroupByElements() {
        return groupByElements;
    }    
}