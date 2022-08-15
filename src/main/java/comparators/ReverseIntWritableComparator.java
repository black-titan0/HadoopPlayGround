package comparators;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ReverseIntWritableComparator extends WritableComparator {

    protected ReverseIntWritableComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        IntWritable key1 = (IntWritable) w1;
        IntWritable key2 = (IntWritable) w2;
        return -1 * key1.compareTo(key2);
    }
}
