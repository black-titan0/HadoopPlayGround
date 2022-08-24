package drivers;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sleep.Sleep;
import sleep.Sleep.*;


import static constants.IOConstants.FINAL_OUTPUT_PATH;
import static constants.IOConstants.INPUT_PATH;

public class SleepMapDriver extends Configured implements Tool {

    private static final Logger LOGGER = LoggerFactory.getLogger(SleepMapDriver.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Program Started!");
        int res = ToolRunner.run(new SleepMapDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] strings) throws Exception {
        String inputPathString = getConf().get(INPUT_PATH);
        String outputPathString = getConf().get(FINAL_OUTPUT_PATH);

        Job sleepJob = Job.getInstance(getConf(), "Sleep Job");
        sleepJob.setJarByClass(Sleep.class);
        sleepJob.setMapperClass(SleepMapper.class);
        sleepJob.setNumReduceTasks(0);
        sleepJob.setMapOutputKeyClass(NullWritable.class);
        sleepJob.setMapOutputValueClass(NullWritable.class);
        sleepJob.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(sleepJob, new Path(inputPathString));
        NLineInputFormat.setNumLinesPerSplit(sleepJob,1);
        FileOutputFormat.setOutputPath(sleepJob, new Path(outputPathString));

        LOGGER.info("Sleep Job Has Been Configured Successfully!");
        boolean success = sleepJob.waitForCompletion(true);
        return (success) ? 1 : 0;
    }
}
