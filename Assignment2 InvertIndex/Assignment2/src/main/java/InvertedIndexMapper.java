import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

public class InvertedIndexMapper extends Mapper<Object,Text,Text,IntWritable> {
    private Set<String> stopwords;
    private Path[] localFiles;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        stopwords = new TreeSet<String>();
        Configuration conf = context.getConfiguration();
        localFiles = DistributedCache.getLocalCacheFiles(conf);
        for (int i = 0; i < localFiles.length; i++) {
            String line;
            BufferedReader br = new BufferedReader(new FileReader(localFiles[i].toString()));
            while ((line = br.readLine()) != null)
            {
                StringTokenizer itr = new StringTokenizer(line);
                while(itr.hasMoreTokens()){
                    stopwords.add(itr.nextToken());
                }
            }
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        fileName = fileName.replaceAll(".txt.segmented", "");
        fileName = fileName.replaceAll(".TXT.segmented", "");
        String temp = new String();
        String line = value.toString().toLowerCase();
        StringTokenizer itr=new StringTokenizer(line);
        for (;itr.hasMoreTokens();){
            temp=itr.nextToken();
            if(!stopwords.contains(temp)){
                Text word = new Text();
                word.set(temp + "#" +fileName);
                context.write(word,new IntWritable(1));
            }
        }
    }
}
