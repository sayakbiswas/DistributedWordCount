/**
 * Created by sayak on 23/9/16.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class DistributedWordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private ArrayList<String> wordList = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (context.getCacheFiles() != null
                    && context.getCacheFiles().length > 0) {
                File cacheFile = new File("./word-patterns.txt"); //Read the Cache file
                BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(cacheFile)));
                String line;
                while((line = br.readLine()) != null) { //Normal Line parsing
                    StringTokenizer itr = new StringTokenizer(line);
                    while(itr.hasMoreTokens()) {
                        wordList.add(itr.nextToken());
                    }
                }
            }
            super.setup(context);
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            for (String string: wordList) { //For each word in word-patterns.txt
                StringTokenizer itr = new StringTokenizer(value.toString());
                while (itr.hasMoreTokens()) {
                    String token = itr.nextToken();
                    if(token.equals(string)) { //Find if the word exists in bible.gz
                        word.set(token);
                        context.write(word, one);
                    }
                }
            }
        }
    }

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
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: distributedwordcount <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);

        job.addCacheFile(new Path("s3://cloud-assgn1-b2/word-patterns.txt").toUri()); //Add the Distributed Cache
        job.setJarByClass(DistributedWordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
