import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.*;
import reversesort.ReverseSort.ReverseSortMapper;
import reversesort.ReverseSort.ReverseSortReducer;
import wordcount.WordCount.WordCountMapper;
import wordcount.WordCount.WordCountReducer;

import java.io.IOException;
import java.util.Arrays;


public class MapReduceTests {

    @Test
    public void testWordCountMap() throws IOException {
        String sampleInput = "Salam Salam Chetori? Khoobi?" +
                " Salam Salamati Miare. Migi Chetori? Nemidoonam!";

        new MapDriver<LongWritable, Text, Text, IntWritable>(new WordCountMapper())
                .withInput(new LongWritable(0) ,new Text(sampleInput))
                .withOutput(new Text("Salam"), new IntWritable(1))
                .withOutput(new Text("Salam"), new IntWritable(1))
                .withOutput(new Text("Chetori?"), new IntWritable(1))
                .withOutput(new Text("Khoobi?"), new IntWritable(1))
                .withOutput(new Text("Salam"), new IntWritable(1))
                .withOutput(new Text("Salamati"), new IntWritable(1))
                .withOutput(new Text("Miare."), new IntWritable(1))
                .withOutput(new Text("Migi"), new IntWritable(1))
                .withOutput(new Text("Chetori?"), new IntWritable(1))
                .withOutput(new Text("Nemidoonam!"), new IntWritable(1))
                .runTest();
    }

    @Test
    public void testWordCountReducer() throws IOException {
        new ReduceDriver<Text, IntWritable,Text, IntWritable>(new WordCountReducer())
                .withInput(new Text("test") , Arrays.asList(new IntWritable(1),new IntWritable(1),new IntWritable(1)))
                .withOutput(new Text("test"), new IntWritable(3))
                .runTest();
    }

    @Test
    public void testReverseSortMap() throws IOException {
        new MapDriver<Text, Text, IntWritable, Text>(new ReverseSortMapper())
                .withInput(new Text("Salam") , new Text("3"))
                .withInput(new Text("Hello") , new Text("   2"))
                .withOutput(new IntWritable(3), new Text("Salam"))
                .withOutput(new IntWritable(2), new Text("Hello"))
                .runTest();

    }

    @Test
    public void testReverseSortReducer() throws IOException {
        new ReduceDriver<IntWritable, Text, Text, IntWritable>(new ReverseSortReducer())
                .withInput(new IntWritable(2), Arrays.asList(new Text("Salam"),new Text("Hello")))
                .withInput(new IntWritable(1), Arrays.asList(new Text("Marhaba"), new Text("Bonjour")))
                .withOutput(new Text("Salam"), new IntWritable(2))
                .withOutput(new Text("Hello"), new IntWritable(2))
                .withOutput(new Text("Marhaba"), new IntWritable(1))
                .withOutput(new Text("Bonjour"), new IntWritable(1))
                .runTest();
    }
}
