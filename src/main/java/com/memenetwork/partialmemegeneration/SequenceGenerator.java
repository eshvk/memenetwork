package com.memenetwork.partialmemegeneration;
import com.memenetwork.common.io.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceGenerator extends Configured implements Tool {
    public int run (String [] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int numReduceTasks = Integer.parseInt(args[2]);
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", false);
        Job job = new Job(conf,"Sequence Generator");
        job.setJarByClass(SequenceGenerator.class);
        job.setMapperClass(SequenceGeneratorMapper.class);
        job.setReducerClass(SequenceGeneratorReducer.class);
        job.setMapOutputKeyClass(BucketPiece.class);
        job.setMapOutputValueClass(Bucket.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SequenceArrayWritable.class);
        job.setPartitionerClass(SequenceGeneratorPartitioner.class);
        job.setNumReduceTasks(numReduceTasks);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath) ){
            fs.delete(outputPath);
        }
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)){
            return 0;
        }
        else {
            return 1;
        }
    }
    public static void main (String [] args) throws Exception {
        System.exit(ToolRunner.run(new SequenceGenerator(), args));
    }
}
