package comparators;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseLongWritableComparator extends WritableComparator {

    protected ReverseLongWritableComparator() {
        super(LongWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        LongWritable key1 = (LongWritable) w1;
        LongWritable key2 = (LongWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}
