package expressq2.model;


/**
 * This class Models an operation annotation. A Operation annotation appears in a 
 * Query PatterNode and indicates that the node carries an operation. This 
 * operation applies to the node at the time of translating the QueryPattern to 
 * a query. The Operation can be ether an Aggregation or a GroupBy.
 */
public class OperationAnnotation {
    public String column;     // The Column where the Operation is applied.
    public String alias;      // The Alias of the OperationAnnotation when translated to SQL.
    public Keyword operator;  // The Operator.

    /** Constructor */
    public OperationAnnotation(String column, Keyword operator) {
        this.column = column;
        this.operator = operator;
        this.alias = this.operator.getTerm().toLowerCase() + this.column;
    }


    /**
     * @return the column
     */
    public String getColumn() {
        return this.column;
    }

    /**
     * @return the aggregateTerm
     */
    public Keyword getOperator() {
        return this.operator;
    }

    /**
     * @return the alias
     */
    public String getAlias() {
        return this.alias;
    }

    /** Print AggregateAnnotation. */
    @Override
    public String toString() {
        return this.operator.getTerm() + "(" + this.column + ")";
    }
}