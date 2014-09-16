package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int _perPageCost;
    private int _tableId;
    private TupleDesc _td;
    private int _numTuples;
    private Object[] _histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        _perPageCost = ioCostPerPage;
        _tableId = tableid;
        _numTuples = 0;

        Catalog catalog = Database.getCatalog();
        _td =  catalog.getTupleDesc(tableid);

        _histograms = new Object[_td.numFields()];
        boolean hasIntField = false;

        for (int i = 0; i < _td.numFields(); i++) {
            if (_td.getFieldType(i) == Type.INT_TYPE) {
                hasIntField = true;
            } else {
                _histograms[i] = new StringHistogram(NUM_HIST_BINS);
            }
        }

        SeqScan scan = new SeqScan(new TransactionId(), tableid);

        try {
            // need a SeqScan to collect min & max of int field
            if (hasIntField) {
                int[] mins = new int[_td.numFields()];
                int[] maxs = new int[_td.numFields()];
                for (int i = 0; i < _td.numFields(); i++) {
                    mins[i] = Integer.MAX_VALUE;
                    maxs[i] = Integer.MIN_VALUE;
                }

                scan.open();
                while (scan.hasNext()) {
                    Tuple tuple = scan.next();
                    for (int i = 0; i < _td.numFields(); i++) {
                        if (_td.getFieldType(i) == Type.INT_TYPE) {
                            int val = ((IntField) tuple.getField(i)).getValue();
                            if (mins[i] > val) {
                                mins[i] = val;
                            }
                            if (maxs[i] < val) {
                                maxs[i] = val;
                            }
                        }
                    }
                }
                scan.close();

                // init histogram for int field
                for (int i = 0; i < _td.numFields(); i++) {
                    if (_td.getFieldType(i) == Type.INT_TYPE) {
                        _histograms[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
                    }
                }
            }

            // add field value to its histogram
            scan.open();
            while (scan.hasNext()) {
                Tuple tuple = scan.next();
                _numTuples++;
                for (int i = 0; i < _td.numFields(); i++) {
                    if (_td.getFieldType(i) == Type.INT_TYPE) {
                        IntHistogram histogram = (IntHistogram) _histograms[i];
                        histogram.addValue(((IntField) tuple.getField(i)).getValue());
                    } else {
                        StringHistogram histogram = (StringHistogram) _histograms[i];
                        histogram.addValue(((StringField) tuple.getField(i)).getValue());
                    }
                }
            }
            scan.close();

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("failed to scan for statistics collection", e);
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDbFile(_tableId);
        return heapFile.numPages() * _perPageCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (_td.getFieldType(field) == Type.INT_TYPE) {
            int val = ((IntField) constant).getValue();
            IntHistogram histogram = (IntHistogram) _histograms[field];
            return histogram.estimateSelectivity(op, val);

        } else {
            String val = ((StringField) constant).getValue();
            StringHistogram histogram = (StringHistogram) _histograms[field];
            return histogram.estimateSelectivity(op, val);
        }
    }

    public double estimateSelectivity(Predicate predicate) {
        return estimateSelectivity(predicate.getField(), predicate.getOp(), predicate.getOperand());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return _numTuples;
    }

}
