import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    /**
     * TokenizerMapper 继续自 Mapper<Object, Text, Text, IntWritable>
     *
     * [一个文件就一个map,两个文件就会有两个map]
     * map[这里读入输入文件内容 以" \t\n\r\f" 进行分割，然后设置 word ==> one 的key/value对]
     *
     * Writable的主要特点是它使得Hadoop框架知道对一个Writable类型的对象怎样进行serialize以及deserialize.
     * WritableComparable在Writable的基础上增加了compareT接口，使得Hadoop框架知道怎样对WritableComparable类型的对象进行排序。
     *
     * @author yangchunlong.tw
     *
     */
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    /**
     * IntSumReducer 继承自 Reducer<Text,IntWritable,Text,IntWritable>
     *
     * [不管几个Map,都只有一个Reduce,这是一个汇总]
     * reduce[循环所有的map值,把word ==> one 的key/value对进行汇总]
     *
     * 这里的key为Mapper设置的word[每一个key/value都会有一次reduce]
     *
     * 当循环结束后，最后的确context就是最后的结果.
     *
     * @author yangchunlong.tw
     *
     */
    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "157286400");
        conf.setBoolean("mapred.output.compress", true);
        Job job = new Job(conf);
        job.setJobName("MergeFiles");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setInputFormatClass(CombineSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(BytesWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(0);

        System.exit(job.waitForCompletion(true) ? 0 : 1);//等待完成退出.
    }
}  