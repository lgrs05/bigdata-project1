package edu.uprm.cse.bigdata.Retweets;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class RetweetsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String ids = "";
        for(Text value : values)
            if(value.toString().compareTo(key.toString()+"n")==0)
                ids += "Not retweeted";
            else
                ids += " "+ value.toString();

            context.write(key, new Text(ids));


    }

}
