package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbField;
    private Type _gbFieldType;
    private int _aggField;
    private Op _aggOp;

    private Map<Field, Integer> _groupResult;
    private Map<Field, Integer> _groupCount; // only for AVG

    private static final Field DEFAULT_GROUP = new IntField(0);

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        _gbField = gbfield;
        _gbFieldType = gbfieldtype;
        _aggField = afield;
        _aggOp = what;

        _groupResult = new HashMap<Field, Integer>();
        _groupCount = new HashMap<Field, Integer>();
    }

    public boolean isGroupAggregator() {
        return _gbField != Aggregator.NO_GROUPING;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupKey = isGroupAggregator() ? tup.getField(_gbField) : DEFAULT_GROUP;

        int newValue = ((IntField) tup.getField(_aggField)).getValue();
        Integer oldValue = _groupResult.get(groupKey);

        switch (_aggOp) {
            case MIN:
                if (oldValue == null)
                    oldValue = Integer.MAX_VALUE;
                _groupResult.put(groupKey, Math.min(oldValue, newValue));
                break;

            case MAX:
                if (oldValue == null)
                    oldValue = Integer.MIN_VALUE;
                _groupResult.put(groupKey, Math.max(oldValue, newValue));
                break;

            case SUM:
                if (oldValue == null)
                    oldValue = 0;
                _groupResult.put(groupKey, oldValue + newValue);
                break;

            case AVG:
                if (oldValue == null) {
                    _groupResult.put(groupKey, newValue);
                    _groupCount.put(groupKey, 1);

                } else {
                    _groupResult.put(groupKey, oldValue + newValue);
                    _groupCount.put(groupKey, 1 + _groupCount.get(groupKey));
                }
                break;

            case COUNT:
                if (oldValue == null)
                    _groupResult.put(groupKey, 1);
                else
                    _groupResult.put(groupKey, 1 + oldValue);
                break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // client should get this iterator after all tuples have been
        // passed into mergeTupleIntoGroup
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

                switch (_aggOp) {
                    case MIN:
                    case MAX:
                    case SUM:
                    case COUNT:
                        if (isGroupAggregator())
                            return makeResultTuple(entry.getKey(), entry.getValue());
                        else
                            return makeResultTuple(entry.getValue());

                    case AVG:
                        // entry.getValue is the group sum in this case
                        int avg = entry.getValue() / _groupCount.get(entry.getKey());
                        if (isGroupAggregator())
                            return makeResultTuple(entry.getKey(), avg);
                        else
                            return makeResultTuple(avg);

                    default:
                        throw new AssertionError("shouldn't get here");
                }
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
