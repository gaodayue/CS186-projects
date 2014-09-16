package simpledb;

/** A LogicalJoinNode represents the state needed of a join of two
 * tables in a LogicalQueryPlan */
public class LogicalJoinNode {

    /** The first table to join (may be null). It's the alias of the table (if no alias, the true table name) */
    public String t1Alias;

    /** The second table to join (may be null).  It's the alias of the table, (if no alias, the true table name).*/
    public String t2Alias;
    
    /** The name of the field in t1 to join with. It's the pure name of a field, rather that alias.field. */
    public String f1Name;

    /** The name of the field in t2 to join with. It's the pure name of a field.*/
    public String f2Name;

    public String f1QualifiedName;
    public String f2QualifiedName;
    
    /** The join predicate */
    public Predicate.Op p;

    public LogicalJoinNode(String tbl1Alias, String tbl2Alias, String joinField1, String joinField2, Predicate.Op pred) {
        t1Alias = tbl1Alias;
        t2Alias = tbl2Alias;
        f1Name = joinField1;
        f2Name = joinField2;
        p = pred;
        f1QualifiedName = t1Alias + "." + f1Name;
        f2QualifiedName = t2Alias + "." + f2Name;
    }
    
    /** Return a new LogicalJoinNode with the inner and outer (t1.f1
     * and t2.f2) tables swapped. */
    public LogicalJoinNode swapInnerOuter() {
        Predicate.Op newp;
        if (p == Predicate.Op.GREATER_THAN)
            newp = Predicate.Op.LESS_THAN;
        else if (p == Predicate.Op.GREATER_THAN_OR_EQ)
            newp = Predicate.Op.LESS_THAN_OR_EQ;
        else if (p == Predicate.Op.LESS_THAN)
            newp = Predicate.Op.GREATER_THAN;
        else if (p == Predicate.Op.LESS_THAN_OR_EQ)
            newp = Predicate.Op.GREATER_THAN_OR_EQ;
        else 
            newp = p;
        
        LogicalJoinNode j2 = new LogicalJoinNode(t2Alias,t1Alias, f2Name, f1Name, newp);
        return j2;
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        return (j2.t1Alias.equals(t1Alias)  || j2.t1Alias.equals(t2Alias)) && (j2.t2Alias.equals(t1Alias)  || j2.t2Alias.equals(t2Alias));
//        return j2.t1Alias.equals(t1Alias) &&
//                j2.t2Alias.equals(t2Alias) &&
//                j2.f1Name.equals(f1Name) &&
//                j2.f2Name.equals(f2Name);
    }
    
    @Override public String toString() {
        return t1Alias + ":" + t2Alias ;//+ ";" + f1 + " " + p + " " + f2;
    }
    
    @Override public int hashCode() {
        // FIXME against the general contract that equal objects must have equal hash codes
        return t1Alias.hashCode() + t2Alias.hashCode() + f1Name.hashCode() + f2Name.hashCode();
    }
}




