package shared.database.model;



/**
 * This class represents a Query. It offers an integrated builder pattern for easy Query Creation.
 */
public class Query {
    public static enum Operation {eq, gr, gte, ls, lte };

    SQLDatabase database;

    String select;
    String from;
    String where;
    Integer limit;


    /** Public constructor */
    public Query(SQLDatabase database) {
        this.database = database;
        this.select = "";
        this.from = "";
        this.where = "";
        this.limit = null;
    }

    /**
     * Adds the param string
     *
     * @param selectClause The select clause to add.
     * @return Returns this Query object.
     */
    public Query select(String selectClause) {
        this.select = selectClause;
        return this;
    }

    /**
     *
     * @param fromClause
     * @return
     */
    public Query from(String fromClause) {
        this.from = fromClause;
        return this;
    }


    /**
     *
     * @param whereClause
     * @return
     */
    public Query where(String whereClause) {
        this.where = whereClause;
        return this;
    }

    public Query limit(Integer limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Combine tha above to an SQL query.
     *
     */
    public String toSQL() {
        return String.format(SQLQueries.SQL_SELECT_QUERY, this.select, this.from) +
            // The optional where clause
            (this.where.length() != 0 ? String.format(SQLQueries.SQL_WHERE_CLAUSE, this.where) : "") +
            (this.limit != null ? String.format(SQLQueries.LIMIT_STATEMENT, this.limit) : "" );
    }

    /**
     *
     * Builder functions
     *
    */


    /**
     * Return a SelectBuild object allowing for detailed building.
     * @return
     */
    public SelectBuilder select() {
        return new SelectBuilder(this);
    }

    /**
     * Return a SelectBuild object allowing for detailed building.
     * @return
     */
    public FromBuilder from() {
        return new FromBuilder(this);
    }

    /**
     * Return a SelectBuild object allowing for detailed building.
     * @return
     */
    public WhereBuilder where() {
        return new WhereBuilder(this);
    }


    /**
     *
     * Builder Implementations
     *
    */


    /**
     * This class is responsible for building the select part
     */
    public static class SelectBuilder {
        Query query;  // Hold the query.

        /** Public Constructor */
        public SelectBuilder(Query query) {
            this.query = query;
        }

        /**
         * Adds SQLColumn to the selectClause of the query.
         *
         * @param column
         * @return Return this StringBuilder.
         */
        public SelectBuilder addColumn(SQLColumn column) {
            // TODO store columns for compelcated queries.
            this.query.select += column.getName() + ", ";   // the last column would contain an ", " that we will need to remove
            return this;
        }

        /**
         * Adds SQLColumns to the selectClause of the query.
         *
         * @param columns
         * @return Return this SelectBuilder.
         */
        public SelectBuilder addColumns(SQLColumn... columns) {
            // TODO store columns for compelcated queries.
            for (SQLColumn column: columns)
                this.query.select += column.getName() + ", ";   // the last column would contain an ", " that we will need to remove
            return this;
        }

        /**
         * Builds the select clause and returns the Query.
         * @return
         */
        public Query endSelect() {
            this.query.select = this.query.select.substring(0, this.query.select.length() - 2);  // length( ", " ) = 2
            return this.query;
        }
    }



    /**
     * This class is responsible for building the from part
     */
    public static class FromBuilder {
        Query query;  // Hold the query.
        int aliasesCounter;

        /** Public Constructor */
        public FromBuilder(Query query) {
            this.query = query;
            this.aliasesCounter = 0;
        }

        /**
         * Adds SQLTable to the fromClause of the query.
         *
         * @param table
         * @return Return this FromBuilder.
         */
        public FromBuilder addTable(SQLTable table) {
            // TODO store tables for complicated queries.
            this.query.from += table.getName() + ", ";   // the last table would contain an ", " that we will need to remove
            return this;
        }
        public FromBuilder addTableWithAlias(SQLTable table) {
            // TODO store tables for complicated queries.
            this.query.from += table.getName() + " a" + aliasesCounter++ + ", ";   // the last table would contain an ", " that we will need to remove
            return this;
        }
        public FromBuilder addTable(String table) {
            // TODO store tables for complicated queries.
            this.query.from += table + ", ";   // the last table would contain an ", " that we will need to remove
            return this;
        }

         /**
         * Adds SQLTables to the fromClause of the query.
         *
         * @param tables
         * @return Return this FromBuilder.
         */
        public FromBuilder addTables(SQLTable... tables) {
            // TODO store tables for complicated queries.
            for (SQLTable table: tables)
                this.query.from += table.getName() + ", ";   // the last table would contain an ", " that we will need to remove
            return this;
        }

        /**
         * Builds the select clause and returns the Query.
         * @return
         */
        public Query endFrom() {
            this.query.from = this.query.from.substring(0, this.query.from.length() - 2);  // length( ", " ) = 2
            return this.query;
        }
    }


       /**
     * This class is responsible for building the from part
     */
    public static class WhereBuilder {
        Query query;  // Hold the query.

        /** Public Constructor */
        public WhereBuilder(Query query) {
            this.query = query;
        }


        /**
         * Adds a simple condition, with an operation. The value to use must be
         * passed using a prepared stmt.
         *
         * @param column
         * @param op
         * @return
         */
        public WhereBuilder addSimpleCond(SQLColumn column, Operation op) {
            String symbol = "";
            if (op == Operation.eq) symbol = "=";
            else if (op == Operation.gr) symbol = ">";
            else if (op == Operation.gte) symbol = ">=";
            else if (op == Operation.ls) symbol = "<";
            else if (op == Operation.lte) symbol = "<=";

            this.query.where +=  column.getName() +  symbol + "?" +
                " AND ";
            return this;
        }
        public WhereBuilder addJoinCond(String colA, String colB) {
            this.query.where +=  colA +  " = " + colB +
                " AND ";
            return this;
        }


        /**
         * Add an inverted index condition. The value to search must be
         * passed using a prepared stmt.
         *
         * @param column
         * @param keyword
         * @return
         */
        public WhereBuilder addInvIndexCond(SQLColumn column) {
            this.query.where += this.query.database.getInvIndexCondition().setColumn(column.getName()).build()
                  +
                " AND ";
            return this;
        }
        public WhereBuilder addInvIndexCond(SQLColumn column, String kw) {
            this.query.where += this.query.database.getInvIndexCondition().setColumn(column.getName()).setSearchPhrase(kw) .build()
                  +
                " AND ";
            return this;
        }
        public WhereBuilder addInvIndexCond(String column, String kw) {
            this.query.where += this.query.database.getInvIndexCondition().setColumn(column).setSearchPhrase(kw) .build()
                  +
                " AND ";
            return this;
        }

        /**
         * Adds like condition. The value to search must be
         * passed using a prepared stmt.
         *
         * @param column
         * @return
         */
        public WhereBuilder addLikeCond(SQLColumn column) {
            this.query.where += String.format(
                SQLQueries.LIKE_STMT, column.getName())
                +
                " AND ";
            return this;
        }

        /**
         * Builds the select clause and returns the Query.
         * @return
         */
        public Query endWhere() {
            this.query.where = this.query.where.substring(0, this.query.where.length() - 5);  // length( " AND " ) = 5
            return this.query;
        }
    }
}