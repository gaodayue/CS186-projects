package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;

    // TupleDesc of returned tuple, which is a
    // 1-field tuple containing the number of inserted records
    private TupleDesc _td;

    // an Delete operator is active after opened, and becomes
    // inactive when client calls next.
    private boolean _active;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        _tid = t;
        _child = child;
        _td = new TupleDesc(new Type[] { Type.INT_TYPE },
                            new String[] { "inserted" });
    }

    public TupleDesc getTupleDesc() {
        return _td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        _child.open();
        _active = true;
    }

    public void close() {
        super.close();
        _child.close();
        _active = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.rewind();
        _child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!_active) // already returned result tuple
            return null;

        // delete tuples from child operator
        int numTuples = 0;
        BufferPool bufferPool = Database.getBufferPool();

        while (_child.hasNext()) {
            bufferPool.deleteTuple(_tid, _child.next());
            numTuples++;
        }
        _active = false;
        return newResultTuple(numTuples);
    }

    private Tuple newResultTuple(int numInserted) {
        Tuple resultTuple = new Tuple(getTupleDesc());
        resultTuple.setField(0, new IntField(numInserted));
        return resultTuple;
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
