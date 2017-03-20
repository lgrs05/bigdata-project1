package edu.uprm.cse.bigdata.Replies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class RepliesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String ids = "";
        for(Text value : values)
            if(value.toString().compareTo(key.toString()+"n")==0)
                ids += "Not a reply";
            else
                ids += " "+ value.toString();

            context.write(key, new Text(ids));


    }

}
