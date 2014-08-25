package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate _predicate;
    private DbIterator _childIterator;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        _predicate = p;
        _childIterator = child;
    }

    public Predicate getPredicate() {
        return _predicate;
    }

    public TupleDesc getTupleDesc() {
        return _childIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        _childIterator.open();
    }

    public void close() {
        super.close();
        _childIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.rewind();
        _childIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {

        while (_childIterator.hasNext()) {
            Tuple tuple = _childIterator.next();
            if (_predicate.filter(tuple))
                return tuple;
        }
        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[] { _childIterator };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        throw new UnsupportedOperationException();
    }

}
