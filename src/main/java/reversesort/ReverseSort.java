package reversesort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReverseSort {
    public static class ReverseSortMapper extends Mapper<Text, Text, IntWritable, Text>{
        @Override
        public void map(Text key, Text value, Mapper<Text, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            int number = Integer.parseInt(value.toString().replaceAll("\\s+", ""));
            context.write(new IntWritable(number), key);
        }
    }

    public static class ReverseSortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            for (Text word : values) {
                context.write(word, key);
            }
        }
    }
}
