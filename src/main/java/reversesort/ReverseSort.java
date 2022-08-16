package reversesort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReverseSort {
    public static class ReverseSortMapper extends Mapper<Text, Text, LongWritable, Text>{
        @Override
        public void map(Text key, Text value, Mapper<Text, Text, LongWritable, Text>.Context context) throws IOException, InterruptedException {
            int number = Integer.parseInt(value.toString().replaceAll("\\s+", ""));
            context.write(new LongWritable(number), key);
        }
    }

    public static class ReverseSortReducer extends Reducer<LongWritable, Text, Text, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values, Reducer<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (Text word : values) {
                context.write(word, key);
            }
        }
    }
}
