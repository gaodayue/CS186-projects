package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int _min;
    private int _max;
    private int _bucketSize;
    private int[] _histogram;
    private int _numTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	_bucketSize = (int) Math.ceil((max - min + 1) / (double) buckets);
        _min = min;
        _max = max;
        _histogram = new int[buckets];
        _numTuples = 0;
    }

    private int bucketIndex(int v) {
        return (v - _min) / _bucketSize;
    }

    private int bucketMax(int bucketIndex) {
        return _min + (bucketIndex + 1) * _bucketSize - 1;
    }

    private int bucketMin(int bucketIndex) {
        return _min + bucketIndex * _bucketSize;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	_histogram[bucketIndex(v)]++;
        _numTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucketIndex = bucketIndex(v);
        double estimateTuples;

        switch (op) {
        case EQUALS:
            if (v < _min || v > _max) {
                estimateTuples = 0.0;
            } else {
                estimateTuples = _histogram[bucketIndex] / _bucketSize;
            }
            break;
        case NOT_EQUALS:
            if (v < _min || v > _max) {
                estimateTuples = _numTuples;
            } else {
                estimateTuples = _numTuples - _histogram[bucketIndex] / _bucketSize;
            }
            break;
        case GREATER_THAN:
            if (v < _min) {
                estimateTuples = _numTuples;
            } else if (v > _max) {
                estimateTuples = 0.0;
            } else {
                double bucketFraction = (bucketMax(bucketIndex) - v) / (double) _bucketSize;
                estimateTuples = _histogram[bucketIndex] * bucketFraction;
                for (int i = bucketIndex + 1; i < _histogram.length; i++)
                    estimateTuples += _histogram[i];
            }
            break;
        case GREATER_THAN_OR_EQ:
            if (v < _min) {
                estimateTuples = _numTuples;
            } else if (v > _max) {
                estimateTuples = 0.0;
            } else {
                double bucketFraction = (bucketMax(bucketIndex) - v + 1) / (double) _bucketSize;
                estimateTuples = _histogram[bucketIndex] * bucketFraction;
                for (int i = bucketIndex + 1; i < _histogram.length; i++)
                    estimateTuples += _histogram[i];
            }
            break;
        case LESS_THAN:
            if (v < _min) {
                estimateTuples = 0.0;
            } else if (v > _max) {
                estimateTuples = _numTuples;
            } else {
                double bucketFraction = (v - bucketMin(bucketIndex)) / (double) _bucketSize;
                estimateTuples = _histogram[bucketIndex] * bucketFraction;
                for (int i = 0; i < bucketIndex; i++)
                    estimateTuples += _histogram[i];
            }
            break;
        case LESS_THAN_OR_EQ:
            if (v < _min) {
                estimateTuples = 0.0;
            } else if (v > _max) {
                estimateTuples = _numTuples;
            } else {
                double bucketFraction = (v - bucketMin(bucketIndex) + 1) / (double) _bucketSize;
                estimateTuples = _histogram[bucketIndex] * bucketFraction;
                for (int i = 0; i < bucketIndex; i++)
                    estimateTuples += _histogram[i];
            }
            break;
        default:
            throw new UnsupportedOperationException("unsupported selectivity estimation for predicate " + op);
        }

        return estimateTuples / _numTuples;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return String.format("min=%d, max=%d, bucket_size=%d, bucket_num=%d, num_tuples=%d\n",
                             _min, _max, _bucketSize, _histogram.length, _numTuples);
    }
}
