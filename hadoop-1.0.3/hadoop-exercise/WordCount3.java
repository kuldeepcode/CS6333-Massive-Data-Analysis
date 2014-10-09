import java.io.IOException;
import java.util.*;
import java.util.HashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.fs.FileSystem;


public class WordCount3 {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            String check;
            while (tokenizer.hasMoreTokens()) {
                check = tokenizer.nextToken();
                if (check.length() == 7){
                    word.set(check);
                    context.write(word, one);
                }
            }
        }
    }
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class DecreasingComparator extends Comparator {

        public int compare(WritableComparable a,WritableComparable b){
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            // TODO Auto-generated method stub
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class sortReduce extends Reducer<IntWritable, Text, IntWritable, Text> {
        public void reduce(IntWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            context.write(key, value);
        }

        public void run(Context context) throws IOException, InterruptedException {
            setup(context);
            int count = 0;
            while (context.nextKey() && count++ < 100) {
                reduce(context.getCurrentKey(), context.getValues(), context);
            }
            cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("-------------------");
        System.out.println(args[0]);
        System.out.println("-------------------");
        System.out.println(args[1]);

        Job job=new Job();
        job.setJarByClass(WordCount3.class);
        Path tempath=new Path(args[1]);
        Configuration conf=new Configuration();
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileSystem.get(conf).delete(tempath);
        FileOutputFormat.setOutputPath(job,tempath);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.waitForCompletion(false);

        Job jobsort=new Job();

        FileInputFormat.addInputPath(jobsort, tempath);
        jobsort.setOutputKeyClass(IntWritable.class);
        jobsort.setOutputValueClass(Text.class);
        jobsort.setInputFormatClass(SequenceFileInputFormat.class);

        jobsort.setMapperClass(InverseMapper.class);
        jobsort.setReducerClass(sortReduce.class);
        jobsort.setNumReduceTasks(1);
        Path result=new Path(args[2]);
        FileSystem.get(conf).delete(result);
        FileOutputFormat.setOutputPath(jobsort, result);
        jobsort.setSortComparatorClass(DecreasingComparator.class);

        jobsort.waitForCompletion(false);


    }
}
