package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator _child;
    private TupleDesc _childTd;
    private int _gbField;
    private int _aggField;
    private Aggregator.Op _aggOp;

    private Aggregator _aggregator;
    private DbIterator _aggIt;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    _child = child;
        _childTd = child.getTupleDesc();
        _gbField = gfield;
        _aggField = afield;
        _aggOp = aop;

        Type groupFieldType = null;
        if (_gbField != Aggregator.NO_GROUPING)
            groupFieldType = _childTd.getFieldType(_gbField);

        switch (_childTd.getFieldType(_aggField)) {
            case INT_TYPE:
                _aggregator = new IntegerAggregator(_gbField,
                                                    groupFieldType,
                                                    _aggField,
                                                    _aggOp);
                break;
            case STRING_TYPE:
                _aggregator = new StringAggregator(_gbField,
                                                   groupFieldType,
                                                   _aggField,
                                                   _aggOp);
                break;
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    return _gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    return (_gbField == Aggregator.NO_GROUPING) ?
                    null :
                    _childTd.getFieldName(_gbField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return _aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return _childTd.getFieldName(_aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return _aggOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();

        _child.open();
        while (_child.hasNext()) {
            _aggregator.mergeTupleIntoGroup(_child.next());
        }
        _child.close();

        _aggIt = _aggregator.iterator();
        _aggIt.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        return _aggIt.hasNext() ? _aggIt.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    super.rewind();
        // just rewind the result iterator to
        // avoid redoing the heavy works in open()
        _aggIt.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    String aggColumnName = String.format("%s(%s)",
                                             aggregateFieldName(),
                                             _aggOp.toString());

        if (_gbField == Aggregator.NO_GROUPING)
            return new TupleDesc(new Type[] { Type.INT_TYPE },
                                 new String[] { aggColumnName });

        Type[] typeAr = { _childTd.getFieldType(_gbField), Type.INT_TYPE };
        String[] nameAr = { groupFieldName(), aggColumnName };
        return new TupleDesc(typeAr, nameAr);
    }

    public void close() {
	    super.close();
        _aggIt.close();
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { _child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    _child = children[0];
    }
    
}
