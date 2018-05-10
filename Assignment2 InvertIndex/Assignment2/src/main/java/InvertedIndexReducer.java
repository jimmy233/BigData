import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InvertedIndexReducer
extends Reducer<Text,IntWritable,Text,Text>
{
    private Text word1 = new Text();
    private Text word2 = new Text();
    String temp = new String();
    static Text CurrentItem = new Text(" ");
    static List<String> postingList = new ArrayList<String>();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        word1.set(key.toString().split("#")[0]);
        temp = key.toString().split("#")[1];
        for (IntWritable val:values){
            sum+=val.get();
        }
        word2.set(temp+":"+sum+";");
        if(!CurrentItem.equals(word1)&&!CurrentItem.equals(" "))
        {
            StringBuilder out = new StringBuilder();
            double count = 0;
            double num=0;
            for(String P : postingList){
                out.append(P);
                count += Long.parseLong(P.substring(P.indexOf(":")+1,P.indexOf(";")));
                num++;
            }
            out.insert(0,count/num+",");
            if(count>0)
                context.write(CurrentItem,new Text(out.toString()));
            postingList = new ArrayList<String>();
        }
        CurrentItem = new Text(word1);
        postingList.add(word2.toString());
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder out = new StringBuilder();
        long count=0;
        long num=0;
        for(String P : postingList){
            out.append(P);
            count += Long.parseLong(P.substring(P.indexOf(":")+1,P.indexOf(";")));
            num++;
        }
        out.insert(0,count/num+",");
        if(count>0)
            context.write(CurrentItem,new Text(out.toString()));
    }
}
