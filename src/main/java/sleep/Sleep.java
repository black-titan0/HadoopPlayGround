package sleep;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.time.Duration;

public class Sleep {

    enum Result {
        EXCEPTION,
        SLEPT
    }
    public static class SleepMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            int sleepTime = Integer.parseInt(value.toString());
            context.setStatus("Sleeping for " + sleepTime + "seconds");
            try {
                Thread.sleep(sleepTime * 1000L);
                context.getCounter(Result.SLEPT).increment(1);
            } catch (InterruptedException e) {
                context.getCounter(Result.EXCEPTION).increment(1);
            }
            context.write(new IntWritable(sleepTime), NullWritable.get());
        }
    }
}