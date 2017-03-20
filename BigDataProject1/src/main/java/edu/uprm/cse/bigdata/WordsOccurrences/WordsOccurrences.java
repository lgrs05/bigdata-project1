package edu.uprm.cse.bigdata.WordsOccurrences;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * Created by ubuntu on 2/6/17.
 */
public class WordsOccurrences {
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
        Job job = new Job();

        Path path = new Path(fileUri);
        FileInputFormat.addInputPath(job, path);


        job.setJarByClass(WordsOccurrences.class);
        job.setJobName("Count Schools");


        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(WordsOccurrencesMapper.class);
        job.setReducerClass(WordsOccurrencesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        Job job2 = null;
//        if(job.waitForCompletion(true) ) {
//            //Job job2 = new Job();
//            job2.setJarByClass(edu.uprm.cse.bigdata.WordsOccurrences.WordsOccurrences.class);
//            job2.setJobName("Max State");
//            FileInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000"));
//            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/mapred1"));
//            job2.setMapperClass(edu.uprm.cse.bigdata.MaxMapper.class);
//            job2.setReducerClass(edu.uprm.cse.bigdata.MaxReducer.class);
//            job2.setOutputKeyClass(Text.class);
//            job2.setOutputValueClass(IntWritable.class);
//        }



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
