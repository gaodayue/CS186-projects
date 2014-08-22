package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;
    private int _tableId;

    // TupleDesc of returned tuple, which is a
    // 1-field tuple containing the number of inserted records
    private TupleDesc _td;

    // an Insert operator is active after opened, and becomes
    // inactive when client calls next.
    private boolean _active;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        _tid = t;
        _child = child;
        _tableId = tableid;

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
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!_active) // already returned result tuple
            return null;

        // insert tuples from child operator
        int numTuples = 0;
        BufferPool bufferPool = Database.getBufferPool();

        try {
            while (_child.hasNext()) {
                bufferPool.insertTuple(_tid, _tableId, _child.next());
                numTuples++;
            }
            _active = false;
            return newResultTuple(numTuples);

        } catch (IOException ioe) {
            throw new TransactionAbortedException();
        }
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
