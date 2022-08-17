package matenumbers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FindMate {
    private static int GOAL = 60015625;

    public static void introduceGoal(int introduceGoal) {
        FindMate.GOAL = introduceGoal;
    }

    public static class FindMateMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            int parsedValue = Integer.parseInt(value.toString());
            int mate = GOAL - parsedValue;
            context.write(new IntWritable(Math.min(parsedValue, mate)), new IntWritable(parsedValue));
        }
    }

    public static class FindMateReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            Integer firstSeenMember = null;
            for (IntWritable value : values) {
                int currentValue = value.get();
                if (firstSeenMember == null)
                    firstSeenMember = currentValue;
                else {
                    if (currentValue + firstSeenMember == GOAL) {
                        context.write(new IntWritable(Math.min(currentValue, firstSeenMember)), new IntWritable(Math.max(currentValue, firstSeenMember)));
                        return;
                    }
                }
            }
        }
    }

    public static class FindMateCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
            Integer firstSeenMember = null;
            for (IntWritable value : values) {
                int currentValue = value.get();
                if (firstSeenMember == null)
                    firstSeenMember = currentValue;
                else {
                    if (currentValue + firstSeenMember == GOAL) {
                        context.write(new IntWritable(Math.min(currentValue, firstSeenMember)), new IntWritable(firstSeenMember));
                        context.write(new IntWritable(Math.min(currentValue, firstSeenMember)), new IntWritable(currentValue));
                        return;
                    }
                }
            }
        }
    }
}
