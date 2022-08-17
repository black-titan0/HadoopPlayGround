import matenumbers.FindMate.*;
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

        new MapDriver<LongWritable, Text, Text, LongWritable>(new WordCountMapper())
                .withInput(new LongWritable(0) ,new Text(sampleInput))
                .withOutput(new Text("Salam"), new LongWritable(1))
                .withOutput(new Text("Salam"), new LongWritable(1))
                .withOutput(new Text("Chetori?"), new LongWritable(1))
                .withOutput(new Text("Khoobi?"), new LongWritable(1))
                .withOutput(new Text("Salam"), new LongWritable(1))
                .withOutput(new Text("Salamati"), new LongWritable(1))
                .withOutput(new Text("Miare."), new LongWritable(1))
                .withOutput(new Text("Migi"), new LongWritable(1))
                .withOutput(new Text("Chetori?"), new LongWritable(1))
                .withOutput(new Text("Nemidoonam!"), new LongWritable(1))
                .runTest();
    }

    @Test
    public void testFindMateMap() throws IOException {
        new MapDriver<LongWritable, Text, IntWritable, IntWritable>(new FindMateMapper())
                .withInput(new LongWritable(0) ,new Text("1"))
                .withInput(new LongWritable(0) ,new Text("17"))
                .withInput(new LongWritable(0) ,new Text("148"))
                .withInput(new LongWritable(0) ,new Text("150"))
                .withOutput(new IntWritable(1), new IntWritable(1))
                .withOutput(new IntWritable(17), new IntWritable(17))
                .withOutput(new IntWritable(1), new IntWritable(148))
                .withOutput(new IntWritable(-1), new IntWritable(150))
                .runTest();
    }

    @Test
    public void testWordCountReducer() throws IOException {
        new ReduceDriver<Text, LongWritable,Text, LongWritable>(new WordCountReducer())
                .withInput(new Text("test") , Arrays.asList(new LongWritable(1),new LongWritable(1),new LongWritable(1)))
                .withOutput(new Text("test"), new LongWritable(3))
                .runTest();
    }

    @Test
    public void testReverseSortMap() throws IOException {
        new MapDriver<Text, Text, LongWritable, Text>(new ReverseSortMapper())
                .withInput(new Text("Salam") , new Text("3"))
                .withInput(new Text("Hello") , new Text("   2"))
                .withOutput(new LongWritable(3), new Text("Salam"))
                .withOutput(new LongWritable(2), new Text("Hello"))
                .runTest();

    }

    @Test
    public void testReverseSortReducer() throws IOException {
        new ReduceDriver<LongWritable, Text, Text, LongWritable>(new ReverseSortReducer())
                .withInput(new LongWritable(2), Arrays.asList(new Text("Salam"),new Text("Hello")))
                .withInput(new LongWritable(1), Arrays.asList(new Text("Marhaba"), new Text("Bonjour")))
                .withOutput(new Text("Salam"), new LongWritable(2))
                .withOutput(new Text("Hello"), new LongWritable(2))
                .withOutput(new Text("Marhaba"), new LongWritable(1))
                .withOutput(new Text("Bonjour"), new LongWritable(1))
                .runTest();
    }

    @Test
    public void testFindMateReducer() throws IOException {
        new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>(new FindMateReducer())
                .withInput(new IntWritable(1), Arrays.asList(new IntWritable(1),new IntWritable(148)))
                .withInput(new IntWritable(5), Arrays.asList(new IntWritable(5), new IntWritable(5)))
                .withOutput(new IntWritable(1), new IntWritable(148))
                .runTest();
    }
}
