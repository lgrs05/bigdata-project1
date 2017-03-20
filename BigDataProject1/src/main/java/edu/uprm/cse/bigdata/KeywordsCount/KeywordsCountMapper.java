package edu.uprm.cse.bigdata.KeywordsCount;

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


public class KeywordsCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private List<String> stopWords = Arrays.asList("A", "AN", "AND", "ARE", "AS", "AT", "BE", "BY", "FOR", "FROM"
            + "HAS", "HE", "IN", "IS", "IT", "ITS", "OF", "ON", "THAT", "THE", "TO", "WAS", "WERE", "WILL", "WITH");
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String rawTweet = value.toString();

        try {
            Status status = TwitterObjectFactory.createStatus(rawTweet);
            String text = status.getText().toUpperCase();
            String[] tweetWords = text.split(" ");
            for (int i =0; i<tweetWords.length;i++){
                if(!stopWords.contains(tweetWords[i])){
                    context.write(new Text(tweetWords[i]), new IntWritable(1));
                }
            }

        }
        catch(TwitterException e){

        }
    }
}
