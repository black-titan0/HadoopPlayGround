package drivers;

import matenumbers.FindMate;
import matenumbers.FindMate.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import static constants.IOConstants.*;


public class FindMateNumbersDriver extends Configured implements Tool {

    public static final Logger LOGGER = Logger.getLogger(FindMateNumbersDriver.class);

    public static void main(String[] args) throws Exception {
        LOGGER.log(Level.INFO, "Program Started!");
        int res = ToolRunner.run(new FindMateNumbersDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        LOGGER.log(Level.INFO, "FindMateNumbersDriver Started!");
        String inputPathString = getConf().get(INPUT_PATH),
                outputPathString = getConf().get(FINAL_OUTPUT_PATH);
        LOGGER.log(Level.INFO, "Input And Output Paths Specified: " + inputPathString + " " + outputPathString);

        int goal = Integer.parseInt(getConf().get(GOAL_VALUE));
        LOGGER.log(Level.INFO, "Goal" + goal + " introduced to  job");

        Job findMateJob = Job.getInstance(getConf(), "Find Mate Job");
        findMateJob.setJarByClass(FindMate.class);
        findMateJob.setMapperClass(FindMateMapper.class);
        findMateJob.setReducerClass(FindMateReducer.class);
        findMateJob.setMapOutputKeyClass(IntWritable.class);
        findMateJob.setMapOutputValueClass(IntWritable.class);
        findMateJob.setOutputKeyClass(IntWritable.class);
        findMateJob.setOutputValueClass(IntWritable.class);
        findMateJob.setOutputFormatClass(TextOutputFormat.class);
        findMateJob.setCombinerClass(FindMateCombiner.class);

        FileInputFormat.addInputPath(findMateJob, new Path(inputPathString));
        FileOutputFormat.setOutputPath(findMateJob, new Path(outputPathString));

        LOGGER.log(Level.INFO, "FindMate Has Been Configured Successfully!");
        boolean success = findMateJob.waitForCompletion(true);
        return (success) ? 1 : 0;
    }
}
