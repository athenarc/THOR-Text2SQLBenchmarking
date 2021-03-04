package shared.database.model;

import shared.database.connectivity.DatabaseIndexCreator;
import shared.database.connectivity.DatabaseUtil;
import shared.database.connectivity.PostgreSQLInformationReader;

public class PostgreSQLDatabase extends SQLDatabase {

    /**
     * See {@link InvIdxCondBuilder} for details.
     */
    public static class PostgreSQLInvIdxCondBuilder implements InvIdxCondBuilder {
        String column = "";     // The column used to fill condition.
        String phrase = "?";    // The phrase used to fill condition. Use "?" so if not phrase is set, leave it to be set using a prepared Statement.

        @Override
        public InvIdxCondBuilder setColumn(String col, String TableAlias) {
            this.column = TableAlias + "." + DatabaseIndexCreator.INDEXED_COLUMN_PREFIX + col;
            return this;
        }

        @Override
        public InvIdxCondBuilder setColumn(String col) {
            this.column = DatabaseIndexCreator.INDEXED_COLUMN_PREFIX + col;
            return this;
        }

        @Override
        public InvIdxCondBuilder setSearchPhrase(String phrase) {
            this.phrase = "'" + phrase + "'";
            return this;
        }

        @Override
        public String build() {
            return String.format(PostgreSQLQueries.INV_INDEX_COND, this.column, this.phrase);
        }
    }

    /** Public constructor with name */
    public PostgreSQLDatabase(String name) {
        super(name);
    }

    @Override
    public void fillDatabase() {
        PostgreSQLInformationReader.getTableAndColumnNames(this);
        PostgreSQLInformationReader.getFKConstraints(this);
        PostgreSQLInformationReader.getIndexedColumns(this);
        PostgreSQLInformationReader.getTableAndColumnStatistics(this);
    }

    @Override
    public InvIdxCondBuilder getInvIndexCondition() {
        return new PostgreSQLInvIdxCondBuilder();
    }

    @Override
    public String prepareForAndFullTextSearch(String phrase) {
        return DatabaseUtil.prepareForAndTsQuerySearch(phrase);
    }
 
    

}