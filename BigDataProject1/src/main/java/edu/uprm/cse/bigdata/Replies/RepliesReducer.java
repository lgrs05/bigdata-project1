package edu.uprm.cse.bigdata.Replies;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;


public class RepliesReducer extends Reducer<Text, Text, Text, Text> {
    //File file = new File("replies.json");
    String names ="{\n" +
            "\"nodes\": [";
    String ids = "";
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
       // FileWriter fw = new FileWriter(file);


        for(Text value : values)
                  ids += " "+ value.toString();

            context.write(key, new Text(ids));


    }

}
