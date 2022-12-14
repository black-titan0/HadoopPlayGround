package drivers;

import comparators.ReverseLongWritableComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reversesort.ReverseSort;
import reversesort.ReverseSort.ReverseSortMapper;
import reversesort.ReverseSort.ReverseSortReducer;
import wordcount.WordCount;
import wordcount.WordCount.WordCountMapper;
import wordcount.WordCount.WordCountReducer;

import static constants.IOConstants.*;

public class CountReverseSortDriver extends Configured implements Tool {

    static final Logger LOGGER = LoggerFactory.getLogger(CountReverseSortDriver.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Program Started!");
        int res = ToolRunner.run(new CountReverseSortDriver(), args);
        /*return res;*/
    }

    @Override
    public int run(String[] strings) throws Exception {
        LOGGER.info("CountReverseSortDriver Started!");

        String inputPathString = getConf().get(INPUT_PATH),
                intermediatePath = getConf().get(INTERMEDIATE_OUTPUT_PATH),
                outputPathString = getConf().get(FINAL_OUTPUT_PATH);
        LOGGER.info("Input And Output Paths Specified: {} {} {}", inputPathString, intermediatePath, outputPathString);

        Job countJob = Job.getInstance(getConf(), "Count Job");
        countJob.setJarByClass(WordCount.class);
        countJob.setMapperClass(WordCountMapper.class);
        countJob.setReducerClass(WordCountReducer.class);
        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(LongWritable.class);
        countJob.setOutputKeyClass(Text.class);
        countJob.setOutputValueClass(LongWritable.class);
        countJob.setOutputFormatClass(TextOutputFormat.class);
        countJob.setCombinerClass(WordCountReducer.class);


        FileInputFormat.addInputPath(countJob, new Path(inputPathString));
        FileOutputFormat.setOutputPath(countJob, new Path(intermediatePath));

        LOGGER.info("CountJob Has Been Configured Successfully!");
        boolean success = countJob.waitForCompletion(true);

        if (success) {
            LOGGER.info("CountJob Has Been Executed Successfully!");
            Job sortJob = Job.getInstance(getConf(), "Sort Job");
            sortJob.setInputFormatClass(KeyValueTextInputFormat.class);

            sortJob.setSortComparatorClass(ReverseLongWritableComparator.class);
            LOGGER.debug("Sort Comparator Changed to : {} ", (sortJob.getSortComparator().getClass().getName()));
            sortJob.setJarByClass(ReverseSort.class);
            sortJob.setMapperClass(ReverseSortMapper.class);
            sortJob.setReducerClass(ReverseSortReducer.class);
            sortJob.setMapOutputKeyClass(LongWritable.class);
            sortJob.setMapOutputValueClass(Text.class);
            sortJob.setOutputKeyClass(Text.class);
            sortJob.setOutputValueClass(LongWritable.class);
            sortJob.setOutputFormatClass(TextOutputFormat.class);


            FileInputFormat.addInputPath(sortJob, new Path(intermediatePath));
            FileOutputFormat.setOutputPath(sortJob, new Path(outputPathString));

            LOGGER.info("SortJob Has Been Configured Successfully!");
            success = sortJob.waitForCompletion(true);
            if (success)
                LOGGER.info("SortJob Has Been Executed Successfully!");
        }
        return (success) ? 1 : 0;
    }
}
