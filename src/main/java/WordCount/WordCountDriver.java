package WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //获取配置

//
//        String[] test_args = {"test_in","test_out"};
//        args = test_args;

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定jar包
        job.setJarByClass(WordCountDriver.class);

        //指定map和reduce类
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordCountReduce.class);

        //指定map输出参数
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定输出参数
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定输入目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //指定输出目录
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        //提交任务
        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
