package simpledb;

/** A LogicalSubplanJoinNode represens the state needed of a join of a
 * table to a subplan in a LogicalQueryPlan -- inherits state from
 * {@link LogicalJoinNode}; t2 and f2 should always be null
 */
public class LogicalSubplanJoinNode extends LogicalJoinNode {
    
    /** The subplan (used on the inner) of the join */
    DbIterator subPlan;
    
    public LogicalSubplanJoinNode(String table1, String joinField1, DbIterator sp, Predicate.Op pred) {
        super(table1, null, joinField1, null, pred);
        subPlan = sp;
    }
    
    @Override public int hashCode() {
        return t1Alias.hashCode() + f1Name.hashCode() + subPlan.hashCode();
    }
    
    @Override public boolean equals(Object o) {
        LogicalJoinNode j2 =(LogicalJoinNode)o;
        if (!(o instanceof LogicalSubplanJoinNode))
            return false;
        
        return (j2.t1Alias.equals(t1Alias)  && j2.f1Name.equals(f1Name) && ((LogicalSubplanJoinNode)o).subPlan.equals(subPlan));
    }
    
    public LogicalSubplanJoinNode swapInnerOuter() {
        LogicalSubplanJoinNode j2 = new LogicalSubplanJoinNode(t1Alias, f1Name,subPlan, p);
        return j2;
    }

}
