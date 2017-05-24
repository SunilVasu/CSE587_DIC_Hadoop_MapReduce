/**
 * Created by sunil on 4/23/17.
 */
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TestOldCombinerGrouping;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.naming.Context;

import static com.sun.org.apache.xalan.internal.xsltc.compiler.util.Type.Text;

public class Activity2a {

    public static HashMap<String,ArrayList> lemmaDict=new HashMap<String, ArrayList>();

    public static void createDict(String csvFile){
        BufferedReader br = null;
        String line = "";
        String splitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
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
                    lemmaDict.put(key, val);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(">");
            if(tokens.length >1) {
                if (tokens[0].length() > 0 && tokens[1].length() > 0) {
                    String loc = tokens[0] + ">";

                    String[] subWord = tokens[1].trim().split(" ");
                    for (int i = 0; i < subWord.length; i++) {

                        for (int j = i + 1; j < subWord.length; j++) {
                            if(i==j)
                                continue;
                            if(subWord[i].trim().length()==0)
                                continue;

                            String w = new String();
                            String ch1=subWord[i].toLowerCase().replaceAll("[^A-Za-z]+", "");
                            String ch2=subWord[j].toLowerCase().replaceAll("[^A-Za-z]+", "");

                            ch1 = ch1.replaceAll("j", "i");
                            ch1 = ch1.replaceAll("v", "u");

                            ch2 = ch2.replaceAll("j", "i");
                            ch2 = ch2.replaceAll("v", "u");

                            if(ch1.trim().length()==0)
                                continue;

                            if(ch2.trim().length()==0)
                                continue;
                            //Calling the lemma Dictionary
                            ArrayList<String> lemma1 = new ArrayList<String>();
                            ArrayList<String> lemma2 = new ArrayList<String>();
                            Text word = new Text();
                            Text lemmaLoc = new Text();

                            if (lemmaDict.containsKey(ch1)){

                                lemma1 = lemmaDict.get(ch1);
                            }
                            else{
                                lemma1.add(ch1);
                            }

                            if (lemmaDict.containsKey(ch2)){
                                lemma2 = lemmaDict.get(ch2);
                            }
                            else{
                                lemma2.add(ch2);

                            }

                            for (String var1 : lemma1) {
                                for (String var2 : lemma2) {
                                    word = new Text();
                                    lemmaLoc = new Text();
                                    if(var1.compareTo(var2) >0){
                                        word.set(var2+":"+var1);
                                    }else{
                                        word.set(var1+":"+var2);
                                    }
                                    lemmaLoc.set(loc);
                                    context.write(word, lemmaLoc);
                                    //System.out.println("word:"+word+"  loc"+lemmaLoc);
                                }
                            }

                        }
                    }

                }
            }
        }
    }


//Combiner for efficiency
/*    public static class IntSumCombiner
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String sum = "";
            for (Text val : values) {
                sum = sum+val.toString();
            }

            String[] pair = key.toString().trim().split(":");
            if(pair.length>1){
                if(pair[0].length()>0 && pair[1].length()>0){
                    String w1 = pair[0].replaceAll("[^A-Za-z]+", "").toLowerCase();
                    String w2 = pair[1].replaceAll("[^A-Za-z]+", "").toLowerCase();

                    ArrayList<String> lemma1 = new ArrayList<String>();
                    ArrayList<String> lemma2 = new ArrayList<String>();

                    Text word = new Text();
                    Text lemmaLoc = new Text();

                    if (lemmaDict.containsKey(w1)){

                        lemma1 = lemmaDict.get(w1);
                                            }
                    else{
                        lemma1.add(w1);
                                            }

                    if (lemmaDict.containsKey(w2)){
                        lemma2 = lemmaDict.get(w2);
                        }
                    else{
                        lemma2.add(w2);

                    }

                    for (String var1 : lemma1) {
                        for (String var2 : lemma2) {
                            word = new Text();
                            lemmaLoc = new Text();
                            if(var1.compareTo(var2) >0){
                                word.set(var2+":"+var1);
                            }else{
                                word.set(var1+":"+var2);
                            }
                            lemmaLoc.set(sum);
                            context.write(word, lemmaLoc);
                            //System.out.println("word:"+word+"  loc"+lemmaLoc);
                        }
                    }
                }
            }
        }
    }*/

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String sum = "";
            for (Text val : values) {
                sum = sum+val.toString();
            }
            //System.out.println("Inside Reducer"+"key :"+key+" value:"+sum);
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        FileUtils.deleteDirectory(new File("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q4/out"));
        createDict("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q4/new_lemmatizer.csv");

        job.setJarByClass(Activity2a.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q4/in"));
        FileOutputFormat.setOutputPath(job, new Path("/home/sunil/WorkSpace/R/DIC_Lab4/IO/Q4/out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

