package simpledb;

/** A LogicalFilterNode represents the parameters of a filter in the WHERE clause of a query. 
    <p>
    Filter is of the form t.f p c
    <p>
    Where t is a table, f is a field in t, p is a predicate, and c is a constant
*/
public class LogicalFilterNode {
    /** The alias of a table (or the name if no alias) over which the filter ranges */
    public String tableAlias;

    /** The predicate in the filter */
    public Predicate.Op p;
    
    /* The constant on the right side of the filter */
    public String c;
    
    /** The field from t which is in the filter. The pure name, without alias or tablename*/
    public String fieldName;

    public String qualifiedFieldName;
    
    public LogicalFilterNode(String tableAlias, String fieldName, Predicate.Op pred, String constant) {
        this.tableAlias = tableAlias;
        this.fieldName = fieldName;
        p = pred;
        c = constant;
        qualifiedFieldName = tableAlias + "." + fieldName;
    }
}