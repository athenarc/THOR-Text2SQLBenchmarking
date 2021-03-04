package shared.database.model;

import shared.database.connectivity.DatabaseUtil;
import shared.database.connectivity.MySQLInformationReader;

public class MySqlDatabase extends SQLDatabase {

    /**
     * See {@link InvIdxCondBuilder} for details.
     */
    public static class MySQLInvIdxCondBuilder implements InvIdxCondBuilder {
        String column = "";     // The column used to fill condition.
        String phrase = "?";    // The phrase used to fill condition. Use "?" so if not phrase is set, leave it to be set using a prepared Statement.

        @Override
        public InvIdxCondBuilder setColumn(String col, String TableAlias) {
            this.column = TableAlias + "." + col;
            return this;
        }

        @Override
        public InvIdxCondBuilder setColumn(String col) {
            this.column = col;
            return this;
        }

        @Override
        public InvIdxCondBuilder setSearchPhrase(String phrase) {
            this.phrase = "'" + phrase + "'";
            return this;
        }

        @Override
        public String build() {
            return String.format(SQLQueries.INV_INDEX_CONDITION, this.column, this.phrase);
        }
    }

    /** Public constructor with name */
    public MySqlDatabase(String name) {
        super(name);
    }

    @Override
    public void fillDatabase() {
        MySQLInformationReader.getTableAndColumnNames(this);
        MySQLInformationReader.getFKConstraints(this);
        MySQLInformationReader.getIndexedColumns(this);
        MySQLInformationReader.getTableAndColumnStatistics(this);
    }

    @Override
    public InvIdxCondBuilder getInvIndexCondition() {
        return new MySQLInvIdxCondBuilder();
    }

    /**
     * Call the DatabaseUtil Boolean search preparation function.
     */
    @Override
    public String prepareForAndFullTextSearch(String phrase) {        
        return DatabaseUtil.prepareForAndBooleanSearch(phrase);
    }
}