package edu.uprm.cse.bigdata.Main;

import edu.uprm.cse.bigdata.KeywordsCount.KeywordsCount;
import edu.uprm.cse.bigdata.KeywordsCount.KeywordsCountMapper;
import edu.uprm.cse.bigdata.KeywordsCount.KeywordsCountReducer;
import edu.uprm.cse.bigdata.Replies.Replies;
import edu.uprm.cse.bigdata.Replies.RepliesMapper;
import edu.uprm.cse.bigdata.Replies.RepliesReducer;
import edu.uprm.cse.bigdata.Retweets.Retweets;
import edu.uprm.cse.bigdata.Retweets.RetweetsMapper;
import edu.uprm.cse.bigdata.Retweets.RetweetsReducer;
import edu.uprm.cse.bigdata.ScreenNamesCount.ScreenNamesSet;
import edu.uprm.cse.bigdata.ScreenNamesCount.ScreenNamesSetMapper;
import edu.uprm.cse.bigdata.ScreenNamesCount.ScreenNamesSetReducer;
import edu.uprm.cse.bigdata.TweetsByUser.TweetsByUser;
import edu.uprm.cse.bigdata.TweetsByUser.TweetsByUserMapper;
import edu.uprm.cse.bigdata.TweetsByUser.TweetsByUserReducer;
import edu.uprm.cse.bigdata.WordsOccurrences.WordsOccurrencesMapper;
import edu.uprm.cse.bigdata.WordsOccurrences.WordsOccurrencesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.net.URI;

/**
 * Created by ubuntu on 2/6/17.
 */
public class AllExecutions {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AllExecutions <input path> <output path>");
            System.exit(-1);
        }
        String fileName = args[0];
        String outputFileName = args[1];
        URI fileUri = URI.create(fileName);
        // get the configuration variable
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        // get a handle the underlying hadoop file system
        FileSystem hdfs = FileSystem.get(fileUri, conf);
        InputStream dataIn = null;
        OutputStream dataOut = null;
        long bytesToSend = 0;


        Job job = new Job(conf);
        Path path = new Path(fileUri);
        FileInputFormat.addInputPath(job, path);
        job.setJarByClass(AllExecutions.class);
        job.setJobName("Words Occurrences");
        FileOutputFormat.setOutputPath(job, new Path(args[1]+"/WordsOccurrences"));
        job.setMapperClass(WordsOccurrencesMapper.class);
        job.setReducerClass(WordsOccurrencesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if(job.waitForCompletion(true)) {
            System.out.println(job.getJobName()+" job succeeded");
            try {
                File nf = new File(args[1] + "/WordsOccurrences/Occurrences.csv");
                FileReader fr = new FileReader(new File(args[1] + "/WordsOccurrences/part-r-00000"));
                FileInputStream fis = new FileInputStream(new File(args[1] + "/WordsOccurrences/part-r-00000"));
                BufferedReader bfr = new BufferedReader(fr);
                FileWriter fw = new FileWriter(nf);
                fw.write("Keywords,Occurrences \n");
                String line = bfr.readLine();
                while (line != null) {
                    fw.write(line+"\n");
                    line = bfr.readLine();
                }
                bfr.close();
                fw.close();
            }catch(Exception e){
                System.out.println("Error with file");

            }



        }
        else
            System.out.println(job.getJobName()+" job failed");

        Job secondJob = new Job();
        FileInputFormat.addInputPath(secondJob, path);
        secondJob.setJarByClass(KeywordsCount.class);
        secondJob.setJobName("Keywords Count");
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]+"/KeywordsCount"));
        secondJob.setMapperClass(KeywordsCountMapper.class);
        secondJob.setReducerClass(KeywordsCountReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);

        if(secondJob.waitForCompletion(true))
            System.out.println(secondJob.getJobName()+" job succeeded");
        else
            System.out.println(secondJob.getJobName()+" job failed");

        Job thirdJob = new Job();
        FileInputFormat.addInputPath(thirdJob, path);
        thirdJob.setJarByClass(ScreenNamesSet.class);
        thirdJob.setJobName("Screen Names Set");
        FileOutputFormat.setOutputPath(thirdJob, new Path(args[1]+"/ScreenNamesSet"));
        thirdJob.setMapperClass(ScreenNamesSetMapper.class);
        thirdJob.setReducerClass(ScreenNamesSetReducer.class);
        thirdJob.setOutputKeyClass(Text.class);
        thirdJob.setOutputValueClass(LongWritable.class);

        if(thirdJob.waitForCompletion(true))
            System.out.println(thirdJob.getJobName()+" job succeeded");
        else
            System.out.println(thirdJob.getJobName()+" job failed");

        Job fourthJob = new Job();
        FileInputFormat.addInputPath(fourthJob, path);
        fourthJob.setJarByClass(Retweets.class);
        fourthJob.setJobName("Retweets");
        FileOutputFormat.setOutputPath(fourthJob, new Path(args[1]+"/Retweets"));
        fourthJob.setMapperClass(RetweetsMapper.class);
        fourthJob.setReducerClass(RetweetsReducer.class);
        fourthJob.setOutputKeyClass(Text.class);
        fourthJob.setOutputValueClass(Text.class);

        if(fourthJob.waitForCompletion(true))
            System.out.println(fourthJob.getJobName()+" job succeeded");
        else
            System.out.println(fourthJob.getJobName()+" job failed");

        Job fifthJob = new Job();
        FileInputFormat.addInputPath(fifthJob, path);
        fifthJob.setJarByClass(Replies.class);
        fifthJob.setJobName("Replies");
        FileOutputFormat.setOutputPath(fifthJob, new Path(args[1]+"/Replies"));
        fifthJob.setMapperClass(RepliesMapper.class);
        fifthJob.setReducerClass(RepliesReducer.class);
        fifthJob.setOutputKeyClass(Text.class);
        fifthJob.setOutputValueClass(Text.class);

        if(fifthJob.waitForCompletion(true))
            System.out.println(fifthJob.getJobName()+" job succeeded");
        else
            System.out.println(fifthJob.getJobName()+" job failed");

        Job sixthJob = new Job();
        FileInputFormat.addInputPath(sixthJob, path);
        sixthJob.setJarByClass(TweetsByUser.class);
        sixthJob.setJobName("Tweets by User");
        FileOutputFormat.setOutputPath(sixthJob, new Path(args[1]+"/TweetsByUser"));
        sixthJob.setMapperClass(TweetsByUserMapper.class);
        sixthJob.setReducerClass(TweetsByUserReducer.class);
        sixthJob.setOutputKeyClass(Text.class);
        sixthJob.setOutputValueClass(Text.class);

        if(sixthJob.waitForCompletion(true)) {
            System.out.println(sixthJob.getJobName()+" job succeeded");
            System.exit(0);
        }
        else {
            System.out.println(sixthJob.getJobName()+" job failed");
            System.exit(1);
        }
    }

}
