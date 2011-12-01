package com.memenetwork.common.utils;
import com.memenetwork.finalmemegeneration.MemeGenerator;
import com.memenetwork.memesocialnetwork.TemporalGraphGenerator;
import com.memenetwork.partialmemegeneration.ShingleTableGenerator;
import com.memenetwork.partialmemegeneration.SequenceGenerator;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;
public class MemeDriver extends Configured implements Tool {
    public int run (String [] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];
        String tempDir = args[2];
        String numReduceTasks = args[3];
        String shingleSize = args[4];
        String shingleDir = tempDir + "/shingleTable";
        String [] stgArgs = new String[4];
        stgArgs[0] = inputPath;
        stgArgs[1] = shingleDir;
        stgArgs[2] = numReduceTasks;
        stgArgs[3] = shingleSize;
        ShingleTableGenerator stg = new ShingleTableGenerator();
        int result = stg.run(stgArgs);
        if(result == 1) {
            return 1;
        }
        String [] sgArgs = new String[3];
        String seqDir = tempDir + "/sequences";
        sgArgs[0] = shingleDir;
        sgArgs[1] = seqDir;
        sgArgs[2] = numReduceTasks;
        SequenceGenerator sg = new SequenceGenerator();
        result = sg.run(sgArgs);
        if (result == 1) {
            return 1;
        }
        String [] memeArgs = new String [3];
        String memeDir = tempDir + "/memes";
        memeArgs[0] = seqDir;
        memeArgs[1] = memeDir;
        memeArgs[2] = shingleSize;
        MemeGenerator mg = new MemeGenerator();
        result = mg.run(memeArgs);
        if (result == 1) {
            return 1;
        }
        String [] tgArgs = new String [3];
        tgArgs[0] = memeDir;
        tgArgs[1] = outputPath;
        tgArgs[2] = numReduceTasks;
        TemporalGraphGenerator tg = new TemporalGraphGenerator();
        result = tg.run(tgArgs);
        if (result == 1) {
            return 1;
        }
        else {
            return 0;
        }
   }
    public static void main (String [] args)
        throws Exception{
        System.exit(ToolRunner.run(new MemeDriver(), args));
    }
}
