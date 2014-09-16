package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbField;
    private Type _gbFieldType;
    private int _aggField;
    private Op _aggOp;

    private Map<Field, Integer> _groupResult;
    private static final Field DEFAULT_GROUP = new IntField(0);

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbField = gbfield;
        _gbFieldType = gbfieldtype;
        _aggField = afield;
        _aggOp = what;

        _groupResult = new HashMap<Field, Integer>();
    }

    public boolean isGroupAggregator() {
        return _gbField != Aggregator.NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = isGroupAggregator() ? tup.getField(_gbField) : DEFAULT_GROUP;
        Integer oldValue = _groupResult.get(groupKey);

        switch (_aggOp) {
            case MIN:
            case MAX:
            case SUM:
            case AVG:
                throw new UnsupportedOperationException(
                        "only support COUNT aggregation on STRING field");
            case COUNT:
                if (oldValue == null)
                    _groupResult.put(groupKey, 1);
                else
                    _groupResult.put(groupKey, 1 + oldValue);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        return new DbIterator() {

            TupleDesc resultTd;
            Iterator<Map.Entry<Field, Integer>> it;
            boolean isOpen;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                resultTd = isGroupAggregator() ?
                        new TupleDesc(new Type[] { _gbFieldType, Type.INT_TYPE }) :
                        new TupleDesc(new Type[] { Type.INT_TYPE });

                it = _groupResult.entrySet().iterator();
                isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!isOpen)
                    return false;
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext())
                    throw new NoSuchElementException("no more tuples");

                Map.Entry<Field, Integer> entry = it.next();
                if (isGroupAggregator())
                    return makeResultTuple(entry.getKey(), entry.getValue());
                else
                    return makeResultTuple(entry.getValue());
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return resultTd;
            }

            @Override
            public void close() {
                isOpen = false;
            }

            // make result tuple for grouping aggregator
            private Tuple makeResultTuple(Field groupField, int aggValue) {
                Tuple result = new Tuple(resultTd);
                result.setField(0, groupField);
                result.setField(1, new IntField(aggValue));
                return result;
            }

            // make result tuple for no grouping aggregator
            private Tuple makeResultTuple(int aggValue) {
                Tuple result = new Tuple(resultTd);
                result.setField(0, new IntField(aggValue));
                return result;
            }
        };
    }

}
