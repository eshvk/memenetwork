package com.memenetwork.partialmemegeneration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.Text;
import com.memenetwork.common.io.BucketPiece;
import com.memenetwork.common.io.Bucket;
public class ShingleTableGenerator extends Configured implements Tool {
    public int run (String [] args) throws Exception {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        int numReduceTasks = Integer.parseInt(args[2]);
        int shingleSize = Integer.parseInt(args[3]);
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.output.compress", false);
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setInt("io.sort.mb", 1000);
        conf.setInt("io.sort.factor", 30);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.setInt("shinglesize", shingleSize);
        Job job = new Job(conf,"Shingle Table Generator");
        job.setJarByClass(ShingleTableGenerator.class);
        job.setMapperClass(ShingleTableMapper.class);
        job.setReducerClass(ShingleTableReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BucketPiece.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Bucket.class);
        FileSystem fs = FileSystem.get(conf);
        job.setNumReduceTasks(numReduceTasks);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        if(fs.exists(outputPath) ){
            fs.delete(outputPath);
        }
        for (FileStatus status: fs.listStatus(inputPath)) {
            Path p = status.getPath();
            if(!status.isDir() && !p.getName().startsWith("_")) {
                FileInputFormat.addInputPath(job,p);
            }
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        if (job.waitForCompletion(true)){
            return 0;
        }
        else {
            return 1;
        }
    }
    public static void main (String [] args)
        throws Exception{
        System.exit(ToolRunner.run(new ShingleTableGenerator(), args));
    }
}
