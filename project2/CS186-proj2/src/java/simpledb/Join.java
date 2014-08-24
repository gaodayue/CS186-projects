package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join implements DbIterator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate _predicate;
    private DbIterator _outerChild;
    private DbIterator _innerChild;

    private TupleDesc _resultSchema;
    private DbIterator _joinMethod;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        _predicate = p;
        _outerChild = child1;
        _innerChild = child2;

        _resultSchema = TupleDesc.merge(_outerChild.getTupleDesc(),
                                        _innerChild.getTupleDesc());

        // use HashJoin for equijoin, NestedLoopJoin for others
        if (_predicate.getOperator() == Predicate.Op.EQUALS)
            _joinMethod = new HashJoin();
        else
            _joinMethod = new NestedLoopJoin();
    }

    public JoinPredicate getJoinPredicate() {
        return _predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        return _outerChild.getTupleDesc().getFieldName(_predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        return _innerChild.getTupleDesc().getFieldName(_predicate.getField2());
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        _joinMethod.open();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return _joinMethod.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return _joinMethod.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        _joinMethod.rewind();
    }

    @Override
    public void close() {
        _joinMethod.close();
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return _resultSchema;
    }


    public class NestedLoopJoin extends Operator {
        private Tuple _outerTuple;

        public void open() throws DbException, NoSuchElementException,
                TransactionAbortedException {
            super.open();
            _outerChild.open();
            _innerChild.open();
        }

        public void close() {
            super.close();
            _outerChild.close();
            _innerChild.close();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            _outerChild.rewind();
            _innerChild.rewind();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            while (true) {

                if (_outerTuple == null) {
                    if (_outerChild.hasNext())
                        _outerTuple = _outerChild.next();
                    else
                        return null;
                }


                while (_innerChild.hasNext()) {
                    assert _outerTuple != null;
                    Tuple innerTuple = _innerChild.next();
                    if (_predicate.filter(_outerTuple, innerTuple)) {
                        return Tuple.merge(getTupleDesc(), _outerTuple, innerTuple);
                    }
                }
                _innerChild.rewind();
                _outerTuple = null;
            }
        }

        @Override
        public DbIterator[] getChildren() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChildren(DbIterator[] children) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return _resultSchema;
        }
    }

    // only used in equijoin
    public class HashJoin extends Operator {
        private Map<Field, List<Tuple>> _hashTable = new HashMap<Field, List<Tuple>>();

        private List<Tuple> _nextValues;
        private int _nextPos;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            _innerChild.open();
            // create hash table for the outer table
            _outerChild.open();
            while (_outerChild.hasNext()) {
                Tuple t = _outerChild.next();
                // add tuple to the value list of the given key
                Field key = t.getField(_predicate.getField1());
                if (!_hashTable.containsKey(key)) {
                    _hashTable.put(key, new ArrayList<Tuple>());
                }
                _hashTable.get(key).add(t);
            }
            _outerChild.close();
        }

        @Override
        public void close() {
            super.close();
            _innerChild.close();

            if (_nextValues != null)
                _nextValues.clear();
            _hashTable.clear();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            _innerChild.rewind();
        }

        @Override
        protected Tuple fetchNext() throws DbException, TransactionAbortedException {
            if (_nextValues != null) {
                // use cached result
                Tuple result = _nextValues.get(_nextPos++);
                if (_nextPos == _nextValues.size()) {
                    _nextValues = null;
                    _nextPos = 0;
                }
                return result;
            }

            while (_innerChild.hasNext()) {
                Tuple innerTuple = _innerChild.next();
                Field key = innerTuple.getField(_predicate.getField2());
                if (_hashTable.containsKey(key)) {  // found a match with outer tuple(s)
                    List<Tuple> outTuples = _hashTable.get(key);
                    if (outTuples.size() == 1)
                        return Tuple.merge(getTupleDesc(), outTuples.get(0), innerTuple);
                    // cache results if has more than 1 output tuples
                    _nextValues = new ArrayList<Tuple>();
                    for (Tuple outTuple : outTuples) {
                        _nextValues.add(Tuple.merge(getTupleDesc(), outTuple, innerTuple));
                    }
                    _nextPos = 1;
                    return _nextValues.get(0);

                }
            }
            // no remaining inner tuple match any out tuples.
            return null;
        }

        @Override
        public DbIterator[] getChildren() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChildren(DbIterator[] children) {
            throw new UnsupportedOperationException();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return _resultSchema;
        }
    }
}
