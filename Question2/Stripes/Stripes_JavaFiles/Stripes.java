/**
 * Created by sunil on 4/22/17.
 */
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stripes {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, MyMapWritable>{


        private Text word = new Text();
        private MyMapWritable mapMap = new MyMapWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString());
            String newstr = value.toString().replaceAll("[^A-Za-z]+", " ");
            String[] tokens = newstr.split(" ");
            //String[] tokens = value.toString().split(" ");

            if(tokens.length >1) {
                for (int i = 0; i < tokens.length; i++) {
                    if(tokens[i].trim().length() != 0){
                    word.set(tokens[i]);
                        mapMap = new MyMapWritable();
                    for (int j = 0; j < tokens.length; j++) {
                        if (j == i || tokens[j].trim().length() == 0) continue;

                        if (i != j && tokens[j].trim().length() != 0) {
                            Text strip = new Text(tokens[j]);

                            if(mapMap.containsKey(strip)){
                                IntWritable count = (IntWritable)mapMap.get(strip);
                                count.set(count.get()+1);
                            }else{
                                mapMap.put(strip,new IntWritable(1));
                            }

                        }

                    }
                    if(mapMap.size()!=0)
                        context.write(word, mapMap);
                    }

                 //   System.out.print("Inside Map:"+word+"   "+occurrenceMap+"\n");
                }
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,MyMapWritable,Text,MyMapWritable> {
        private MyMapWritable reducerMap = new MyMapWritable();
        private IntWritable result = new IntWritable();


        public void reduce(Text key, Iterable<MyMapWritable> values,
                           Context context ) throws IOException, InterruptedException {
            System.out.print("Inside Reducer");
            reducerMap = new MyMapWritable();
            for (MyMapWritable val : values) {
                addAll(val);
            }
            System.out.print("Inside Reducer:"+key+"   "+reducerMap+"\n");
            context.write(key, reducerMap);

        }

        private void addAll(MyMapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            for (Writable key : keys) {
                IntWritable fromCount = (IntWritable) mapWritable.get(key);
                if (reducerMap.containsKey(key)) {
                    IntWritable count = (IntWritable) reducerMap.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    reducerMap.put(key, fromCount);
                }
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        FileUtils.deleteDirectory(new File("/home/sunil/WorkSpace/R/DIC_Lab4/IO/out"));
        job.setJarByClass(Stripes.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(MyMapWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}

class MyMapWritable extends MapWritable {
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        Set<Writable> keySet = this.keySet();

        for (Object key : keySet) {
            result.append("{" + key.toString() + " = " + this.get(key) + "}");
        }
        return result.toString();
    }
}
