package expressq2.components;

import shared.database.model.SQLColumn;
import shared.database.model.SQLDatabase;
import shared.database.model.SQLIndexResult;
import shared.database.model.SQLQueries;
import shared.database.model.SQLTable;
import expressq2.ExpressQ2Application;
import expressq2.model.Tag;
import shared.database.connectivity.DataSourceFactory;
import shared.database.connectivity.DatabaseUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


// Input: Set of m keywords {k1, ..., km} and a database.
// Output: Sets of basic tuples for each relation and keyword.
//
// For a keyword k and a relation Ri the corresponding set of basic tuples
// holds all the tuples of Ri which contain the keyword k.
public class DatabaseIndex {

    public static Integer numOfIoSql = 0;

    /**
     * Returns a List of Tags for the keyword parameter. Each tag represents the place
     * where the keyword was found inside the relation. It can be found as an
     * SQLTable name, as an SQLTable Column, or in a tuple value.
     *
     * @param keyword the keyword to search.
     * @param relation the SQLTable that is the search target.
     * @return A List of tags indicating the appearances of a keyword in the relation.
     */
    public static List<Tag> getKeywordsOccurrence(SQLDatabase database, String keyword, SQLTable relation) {
        List<Tag> keywordTags = new ArrayList<>();

        // First check if the keyword equals with the relation name.
        if (keyword.toLowerCase().equals(relation.getName().toLowerCase())) {
            keywordTags.add(new Tag(relation.getName()));
            return keywordTags;
        }


        // Loop all the Attributes (SQLColumns)
        for (SQLColumn attr: relation.getColumns()) {
            // First check if the keyword equals with he relation name.
            if (keyword.toLowerCase().equals(attr.getName().toLowerCase())) {
                keywordTags.add(new Tag(relation.getName(), attr.getName()));
                return keywordTags;
            }

            // Then check if it referenced by a tuple value.
            SQLIndexResult results = database.searchColumn(attr, keyword);
            if (results != null)
                DatabaseIndex.numOfIoSql += results.getTuples().size();
            if (results != null && results.getTuples() != null && results.getTuples().size() > 0) {
                keywordTags.add(new Tag(relation.getName(), attr.getName(), keyword, results.getTuples().size()));
            }
        }

        // Return the Tags.
        return keywordTags;
    }

}
