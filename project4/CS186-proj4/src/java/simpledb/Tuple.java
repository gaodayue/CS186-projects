package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc _tupleDesc;
    private Field[] _fields;
    private RecordId _recordId;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        _tupleDesc = td;
        _fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return _tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        return _recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        _recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        _fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        return _fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        StringBuilder sb = new StringBuilder(_fields[0].toString());
        for (int i = 1; i < _fields.length; i++)
            sb.append("\t").append(_fields[i].toString());
        sb.append("\n");
        return sb.toString();
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {
        return Arrays.asList(_fields).iterator();
    }

    public static Tuple merge(Tuple outerTuple, Tuple innerTuple) {
        TupleDesc newTd = TupleDesc.merge(outerTuple.getTupleDesc(),
                                          innerTuple.getTupleDesc());
        return merge(newTd, outerTuple, innerTuple);
    }

    public static Tuple merge(TupleDesc mergedDesc, Tuple outerTuple, Tuple innerTuple) {
        Tuple newTuple = new Tuple(mergedDesc);
        int i = 0;
        Iterator<Field> it = outerTuple.fields();
        while (it.hasNext()) {
            newTuple.setField(i++, it.next());
        }
        it = innerTuple.fields();
        while (it.hasNext()) {
            newTuple.setField(i++, it.next());
        }
        return newTuple;
    }
}
