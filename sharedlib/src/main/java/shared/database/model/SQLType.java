package shared.database .model;

public class SQLType {

    // The SQL Types in String format.
    private static String SQL_TEXT_STR = "text";
    private static String SQL_LONGTXT_STR = "longtext";
    private static String SQL_VARCHAR_STR = "varchar";
    private static String SQL_INT_STR = "int";
    private static String SQL_DOUBLE_STR = "double";
    private static String SQL_FLOAT_STR = "float";
    
    // Types from PosgteSQL
    private static String PSQL_INT_STR = "integer";
    private static String PSQL_NUM_STR = "numeric";
    private static String PSQL_DATE_STR = "date";
    private static String PSQL_DOUBLE_STR = "double precision";
    private static String PSQL_VARCHAR_STR = "character varying";
    
    
     // 2 Mock SQL types.
    public static SQLType TEXTUAL_TYPE = new SQLType(String.format("%s %s %s %s", SQL_TEXT_STR, SQL_LONGTXT_STR, SQL_VARCHAR_STR, PSQL_VARCHAR_STR) , 0);
    public static SQLType NUMERIC_TYPE = new SQLType("int double float numeric double precision", 0);


    private String type;
    private Long maximumLength;

    public SQLType(String type, Integer maximumLength) {
        this.type = type;
        this.maximumLength = maximumLength.longValue();
    }

    public SQLType(String type, Long maximumLength) {
        this.type = type;
        this.maximumLength = maximumLength;
    }

    // Return the type.
    public String getRawType() {
        return type;
    }
    
    // Return columns maximum length.
    public Long getMaximumLength() {
        return maximumLength;
    }

    // Returns true if type is text-like.
    public boolean isTextual() {
        if (this.type.contains(SQL_VARCHAR_STR) || this.type.contains(SQL_TEXT_STR) || this.type.contains(SQL_LONGTXT_STR) || this.type.contains(PSQL_VARCHAR_STR)) {
            return true;
        }

        return false;
    }

    public boolean isLongText() {
        if (this.type.contains(SQL_LONGTXT_STR)) {
            return true;
        }

        return false;
    }

    public boolean isDate() {
        if (this.type.contains(PSQL_DATE_STR)) {
            return true;
        }

        return false;
    }

    // Return true if type is int.
    public boolean isInt() {
        if (this.type.contains(SQL_INT_STR) || this.type.contains(PSQL_INT_STR)) {
            return true;
        }

        return false;
    }

    public boolean isDouble() {
        if (this.type.contains(SQL_DOUBLE_STR) || this.type.contains(PSQL_DOUBLE_STR)) {
            return true;
        }

        return false;
    }

    public boolean isFloat() {
        if (this.type.contains(SQL_FLOAT_STR)) {
            return true;
        }

        return false;
    }

    // Return true is type is arithmetic.
    public boolean isArithmetic() {
        if (this.type.contains(SQL_INT_STR) || this.type.contains(SQL_DOUBLE_STR) || this.type.contains(SQL_FLOAT_STR) || this.type.contains(PSQL_INT_STR) || this.type.contains(PSQL_DOUBLE_STR) || this.type.contains(PSQL_NUM_STR) ) {
            return true;
        }

        return false;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof SQLType)) return false;
        SQLType t = (SQLType) obj;

        return this.type.contains(t.type) || t.type.contains(this.type);
    }


    @Override
    public String toString() {
        return this.type + ((this.isTextual() && !this.isLongText()) ? ("(" + this.maximumLength.toString() + ")") : (""));
    }

}
