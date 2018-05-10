import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedSort {
    public static class InvertedIndexMapper
            extends Mapper<Object, Text, DoubleWritable, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                String buf = tokenizer.nextToken().toString();
                String str = tokenizer.nextToken().toString();
                String[] split = str.split(",");
                DoubleWritable avg = new DoubleWritable(Double.valueOf(split[0]));
                String outStr = buf+",";
                for (int i = 1; i < split.length; ++i) {
                    outStr += split[i];
                }
                word.set(outStr);
                context.write(avg, word);
            }
        }
    }
    public static class InvertedIndexReducer extends Reducer<DoubleWritable,Text,Text,Text>
    {
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,InterruptedException
    {
        Iterator<Text> it = values.iterator();
        Text newValue = new Text();
        Text newKey = new Text();
        String[] strs;
        while (it.hasNext()){
            //鉴于传到Reduce阶段的时候数据已经排好序了，因而直接将Key(平均出现次数)去掉
            //保留原来的Value并还原成(词语，平均出现次数,文档名:N;...)
            strs = it.next().toString().split(",");
            newKey.set(strs[0]);
            newValue.set(key+","+strs[1]);
            context.write(newKey,newValue);
        }
    }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Inverted Index <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Inverted Sort");
        job.setJarByClass(InvertedSort.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
