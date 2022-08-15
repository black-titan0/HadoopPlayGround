package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
                System.out.println("Map Started");
            for (String word : value.toString().split(" ")) {
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static  class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws InterruptedException, IOException {
            System.out.println("Reduce Started");
            int sum = 0;
            for (IntWritable one : values) {
                sum++;
            }
            context.write(word, new IntWritable(sum));
        }
    }

}
