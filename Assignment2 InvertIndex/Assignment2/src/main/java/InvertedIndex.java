import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.net.URI;
import java.net.URISyntaxException;


public class InvertedIndex {
    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        String text = "hdfs://master01:9000/user/2018st33/Stop-Words.txt";
        //String text="hdfs://localhost:9000/user/Stop-Words.txt";
        try {
            DistributedCache.addCacheFile(new URI(text),conf);
        } catch (URISyntaxException e){
            System.out.println("Error!");
            e.printStackTrace();
        }
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
        if(otherArgs.length!=2)
        {
            System.err.println("Usage:  InvertedIndex <in> <out>");
            System.exit(2);
        }


        Path in = new Path(otherArgs[0]);
        Path out = new Path(otherArgs[1]);
        FileSystem fileSystem = FileSystem.get(new URI(in.toString()), new Configuration());
        if (fileSystem.exists(out)) {
            fileSystem.delete(out, true);
        }

        Job job = new Job(conf,"inverted index");
        job.setJarByClass(InvertedIndex.class);
        try{
            job.setMapperClass(InvertedIndexMapper.class);
            job.setCombinerClass(SumCombiner.class);
            job.setReducerClass(InvertedIndexReducer.class);
            job.setPartitionerClass(NewPartitioner.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }finally {
            FileSystem.get(conf).deleteOnExit(new Path(otherArgs[1]));
        }
    }
}
