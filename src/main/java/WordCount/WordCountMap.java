package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text reduce_key = new Text();
    IntWritable reduce_value = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //assert (false);

        String line = value.toString();
        String[] words = line.split(" ");
        for(String word:words)
        {
            this.reduce_key.set(word);

            context.write(this.reduce_key, this.reduce_value);
        }

    }
}
