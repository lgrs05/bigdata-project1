package edu.uprm.cse.bigdata.ScreenNamesCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;


public class ScreenNamesSetMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String screenName = status.getUser().getScreenName();
            long userId = status.getUser().getId();
            context.write(new Text(screenName), new LongWritable(userId));

        }
        catch(TwitterException e){

        }
    }
}
