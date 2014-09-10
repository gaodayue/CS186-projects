package simpledb;

/** A LogicalSelectListNode represents a clause in the select list in
 * a LogicalQueryPlan
*/
public class LogicalSelectListNode {
    /** The field name being selected; the name may be (optionally) be
     * qualified with a table name or alias.
     */
    public String qualifiedFieldName;
   
    /** The aggregation operation over the field (if any) */
    public String aggOp;

    public LogicalSelectListNode(String aggOp, String qualifiedFieldName) {
        this.aggOp = aggOp;
        this.qualifiedFieldName = qualifiedFieldName;
    }
}
