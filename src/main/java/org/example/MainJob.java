package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class MainJob {
    public static void main(String[] args) throws Exception {
// TODO Auto-generated method stub
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // classe principale
        job.setJarByClass(MainJob.class);

        // classe qui fait le map
        job.setMapperClass(MyMapper.class);

        // classe qui fait le shuffling et le reduce
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // spécifier le fichier d'entrée
        FileInputFormat.addInputPath(job, new Path(args[0]));

// spécifier le fichier contenant le résultat
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
