package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File _file;
    private TupleDesc _td;
    private BufferPool _bufferPool;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        _file = f;
        _td = td;
        _bufferPool = Database.getBufferPool();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return _file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        if (!(pid instanceof HeapPageId))
            throw new IllegalArgumentException("only accept HeapPageId");

        byte[] data = new byte[BufferPool.PAGE_SIZE];
        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(_file, "r");
            raf.seek(BufferPool.PAGE_SIZE * pid.pageNumber());
            raf.readFully(data);
            return new HeapPage((HeapPageId) pid, data);

        } catch (IOException e) {   // better throw IOException
            e.printStackTrace();
            System.exit(1);

        } finally {
            if (raf != null) {
                try {
                    raf.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(_file, "rw");
        raf.seek(BufferPool.PAGE_SIZE * page.getId().pageNumber());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (_file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

        // find a non-full page
        HeapPage page = null;
        boolean found = false;
        for (int pageno = 0; pageno < numPages(); pageno++) {
            page = (HeapPage) _bufferPool.getPage(tid,
                                                  new HeapPageId(getId(), pageno),
                                                  Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                found = true;
                break;
            }
        }


        if (found) {
            page.insertTuple(t);
            page.markDirty(true, tid);

        } else {
            // allocate new page and append to disk
            page = new HeapPage(new HeapPageId(getId(), numPages()),
                                HeapPage.createEmptyPageData());
            page.insertTuple(t);
            this.writePage(page);
        }

        ArrayList<Page> modifiedPages = new ArrayList<Page>();
        modifiedPages.add(page);
        return modifiedPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {

        HeapPage page = (HeapPage) _bufferPool.getPage(tid,
                                                       t.getRecordId().getPageId(),
                                                       Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid, this);
    }
}

