package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator implements DbFileIterator {
    private TransactionId _tid;
    private int _tableId;
    private int _numPages;
    private BufferPool _bufferPool;

    private int _pageNo;
    private HeapPage _page;
    private Iterator<Tuple> _tuples; // tuples iterator of _page

    private boolean _isOpen;

    public HeapFileIterator(TransactionId tid, HeapFile heapFile) {
        _tid = tid;
        _tableId = heapFile.getId();
        _numPages = heapFile.numPages();
        _bufferPool = Database.getBufferPool();
        _isOpen = false;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        _pageNo = 0;
        _page = (HeapPage) _bufferPool.getPage(_tid,
                                               new HeapPageId(_tableId, _pageNo),
                                               Permissions.READ_ONLY);
        _tuples = _page.iterator();
        _isOpen = true;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!_isOpen)
            return false;

        if (_tuples.hasNext())
            return true;

        while (_pageNo < _numPages - 1) {   // has more pages
            // read next page
            _pageNo++;
            _page = (HeapPage) _bufferPool.getPage(_tid,
                                                   new HeapPageId(_tableId, _pageNo),
                                                   Permissions.READ_ONLY);
            _tuples = _page.iterator();
            if (_tuples.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!hasNext())
            throw new NoSuchElementException("no more tuples");
        return _tuples.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public void close() {
        _isOpen = false;
    }
}
