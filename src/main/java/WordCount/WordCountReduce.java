package WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReduce extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable result_count = new IntWritable(0);
    @Override
    protected void reduce(Text key,Iterable<IntWritable> values,Context context)
            throws IOException, InterruptedException {
        //assert (false);

        int count = 0;
        for(IntWritable value:values)
        {
            count += value.get();
        }
        result_count.set(count);
        context.write(key,this.result_count);
    }

}
