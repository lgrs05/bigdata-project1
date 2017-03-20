package edu.uprm.cse.bigdata.ScreenNamesCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ScreenNamesSetReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    List<LongWritable> list = new ArrayList<LongWritable>();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, values.iterator().next());
    }

}
