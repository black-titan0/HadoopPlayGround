package wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context) throws IOException, InterruptedException {
            for (String word : value.toString().split(" ")) {
                context.write(new Text(word), new LongWritable(1L));
            }
        }
    }

    public static  class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        public void reduce(Text word, Iterable<LongWritable> values, Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws InterruptedException, IOException {
            long sum = 0L;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(word, new LongWritable(sum));
        }
    }

}
