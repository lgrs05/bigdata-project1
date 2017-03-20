package edu.uprm.cse.bigdata.Replies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class Replies {
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
        Path path = new Path(fileUri);

        Job fifthJob = new Job();
        FileInputFormat.addInputPath(fifthJob, path);
        fifthJob.setJarByClass(Replies.class);
        fifthJob.setJobName("Replies");
        FileOutputFormat.setOutputPath(fifthJob, new Path(args[1]));
        fifthJob.setMapperClass(RepliesMapper.class);
        fifthJob.setReducerClass(RepliesReducer.class);
        fifthJob.setOutputKeyClass(Text.class);
        fifthJob.setOutputValueClass(Text.class);




        System.exit(fifthJob.waitForCompletion(true) ? 0 : 1);
    }

}
