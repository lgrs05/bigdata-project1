package edu.uprm.cse.bigdata.KeywordsCount;

import edu.uprm.cse.bigdata.WordsOccurrences.WordsOccurrencesMapper;
import edu.uprm.cse.bigdata.WordsOccurrences.WordsOccurrencesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by ubuntu on 2/6/17.
 */
public class KeywordsCount {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordsOccurrences <input path> <output path>");
            System.exit(-1);
        }
        String fileName = args[0];
        String outputFileName = args[1];
        URI fileUri = URI.create(fileName);
        // get the configuration variable
        Configuration conf = new Configuration();
        // get a handle the underlying hadoop file system
        FileSystem hdfs = FileSystem.get(fileUri, conf);
        InputStream dataIn = null;
        OutputStream dataOut = null;
        long bytesToSend = 0;


        Job secondJob = new Job();
        Path path = new Path(fileUri);
        FileInputFormat.addInputPath(secondJob, path);
        secondJob.setJarByClass(KeywordsCount.class);
        secondJob.setJobName("Keywords Count");
        FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));
        secondJob.setMapperClass(KeywordsCountMapper.class);
        secondJob.setReducerClass(KeywordsCountReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(IntWritable.class);




        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
    }

}
