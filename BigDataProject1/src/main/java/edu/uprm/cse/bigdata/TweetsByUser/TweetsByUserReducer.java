package edu.uprm.cse.bigdata.TweetsByUser;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class TweetsByUserReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        long count = 0;
        String ids = "";
        for(Text value : values){
            count++;
            ids+= " " + value.toString();
        }
        String result =  count + ids;


            context.write(key, new Text(result));


    }

}
