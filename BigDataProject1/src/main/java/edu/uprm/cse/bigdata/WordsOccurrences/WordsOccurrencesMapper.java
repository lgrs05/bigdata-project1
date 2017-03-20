package edu.uprm.cse.bigdata.WordsOccurrences;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;


public class WordsOccurrencesMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String text = status.getText().toUpperCase();
            if (text.contains("TRUMP")) context.write(new Text("TRUMP"), new IntWritable(1));

            if (text.contains("MAGA")) context.write(new Text("MAGA"), new IntWritable(1));

            if (text.contains("DICTATOR")) context.write(new Text("DICTATOR"), new IntWritable(1));

            if (text.contains("IMPEACH")) context.write(new Text("IMPEACH"), new IntWritable(1));

            if (text.contains("DRAIN")) context.write(new Text("DRAIN"), new IntWritable(1));

            if (text.contains("SWAMP")) context.write(new Text("SWAMP"), new IntWritable(1));

            if (text.contains("CHANGE")) context.write(new Text("CHANGE"), new IntWritable(1));

        }
        catch(TwitterException e){

        }
    }
}
