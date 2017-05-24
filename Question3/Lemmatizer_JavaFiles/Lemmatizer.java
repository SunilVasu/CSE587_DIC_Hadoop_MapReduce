/**
 * Created by sunil on 4/22/17.
 */


import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lemmatizer {

    public static HashMap<String,ArrayList> lemmaDict=new HashMap<String, ArrayList>();

    public static void createDict(String csvFile){
        //String csvFile = "/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q3/new_lemmatizer.csv";
        BufferedReader br = null;
        String line = "";
        String splitBy = ",";

        try {
            //System.out.println("*Inside createDict*");
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                // use comma as separator
                String[] str = line.split(splitBy);

                if (str[0].length() > 0)
                {
                    String key = str[0];
                    ArrayList val = new ArrayList();
                    for(int i=1; i<str.length;i++) {
                        if(str[i].length()<=0){
                            break;
                        }
                        val.add(str[i]);
                    }
                    //System.out.println("key :"+key+ " val:"+val);
                    lemmaDict.put(key, val);
                }
            }
            //System.out.println("**"+lemmaDict.get(key));
            //return lemmaDict;

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //return null;
    }



    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);


        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //System.out.println("**Inside Map**");

            String[] tokens = value.toString().split(">");
            if(tokens.length >1){
            if(tokens[0].length()>0 && tokens[1].length()>0) {

                String loc = tokens[0] + ">";

                String[] subWord = tokens[1].trim().split(" ");

                for (int j = 0; j < subWord.length; j++) {
                    String w = subWord[j].replaceAll("[^A-Za-z]+", "");
                    w = w.toLowerCase();

                    w = w.replaceAll("j", "i");
                    w = w.replaceAll("v", "u");
                    //System.out.println("text:"+subWord[j]+"loc :"+loc);
                    if (w.trim().length() == 0)
                        continue;
                    if (lemmaDict.containsKey(w)) {
                        Text word = new Text();
                        Text lemmaLoc = new Text();
                        ArrayList<String> lemma = lemmaDict.get(w);
                        for (String var : lemma) {
                            word = new Text();
                            lemmaLoc = new Text();
                            word.set(var);
                            lemmaLoc.set(loc);
                            context.write(word, lemmaLoc);
                            //System.out.println("word:"+word+"  loc"+lemmaLoc);
                        }
                    } else {
                        Text word = new Text();
                        Text lemmaLoc = new Text();
                        word.set(w);
                        lemmaLoc.set(loc);
                        context.write(word, lemmaLoc);
                        //System.out.println("word:"+word+"  loc"+lemmaLoc);
                    }

                }


            }
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            //System.out.println("**********Inside Reducer**********");
            String sum="";
            for (Text val : values) {
                sum = sum+val.toString();
            }
            result.set(sum);
            //System.out.println("**********Exiting Reducer**********");
            context.write(key, result);
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        FileUtils.deleteDirectory(new File("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q3/out"));
        createDict("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q3/new_lemmatizer.csv");
        job.setJarByClass(Lemmatizer.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q3/in"));
        FileOutputFormat.setOutputPath(job, new Path("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q3/out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

