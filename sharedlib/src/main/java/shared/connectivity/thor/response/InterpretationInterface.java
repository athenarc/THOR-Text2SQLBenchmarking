package shared.connectivity.thor.response;

import java.util.Collection;

/**
 * <pre>
 * An interpretation represents an SQL query along with the results produced when executing it against an RDBMS.
 * The interpretation should contain:
 *  + The Query
 *  + A collection of the tables used by the query
 *  + A Table storing the results of the query
 * 
 * From the User side:
 * This interface must be implemented by the system's class responsible for storing results of an SQL query.
 * 
 * From the Application side: 
 * The functions of this interface are used when we need to drain information about an 
 * interpretation and send them to the application's frontend.
 * 
 * </pre>
 */
public interface InterpretationInterface {


    /**
     * A collection that contains the Attribute Names of the results returned by the Query.     
     * NOTE: Basically, this will be the header of the Table when displaying the results.
     */
    public Collection<String> getAttributeNames();


    /**
     * <pre>
     *  A collection of collection that contains the values returned by the Query.
     * 
     * The outer collection represents a "list" of Rows.
     * The inner collection represents a "list" of Values.
     * 
     * An example of the returned object would be :
     *  [
     *    [ Value_of_Row1_Column1, Value_of_Row1_Column2, Value_of_Row1_Column3 ],
     *    [ Value_of_Row2_Column1, Value_of_Row2_Column2, Value_of_Row2_Column3 ],
     *    [ Value_of_Row3_Column1, Value_of_Row3_Column2, Value_of_Row3_Column3 ]
     *  ]
     * </pre>
     */
    public Collection<Collection<String>> getRows();


    /**
	 * A list with the names of the tables that were joined to produce this result.
     */
    public Collection<String> getNetworks();

    /**
     * The SQL query that was executed to produce the result.
     */
    public String getQuery();

}