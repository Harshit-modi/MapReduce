import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class QueryIMDB {

  public static class MapperActors
       extends Mapper<Object, Text, Text, Text>{

    private final static Text outValue = new Text();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
// FileSplit fileSplit = (FileSplit)context.getInputSplit();
// String filename = fileSplit.getPath().getName();

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            String[] result = str.toString().split(";");
            int n = result.length;

            String titleId = result[0];
            String actorId = result[1];
            String actorName = result[2];            

            if (!titleId.equals("\\N") && !actorId.equals("\\N") && !actorName.equals("\\N")) {
                String  midKey = titleId + " " + actorId;
                word.set(midKey);
                outValue.set(actorName);
                context.write(word, outValue);
            }

        }
    }
  }

  public static class MapperDirectors
       extends Mapper<Object, Text, Text, Text>{

    private final static Text one = new Text("1");
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
// FileSplit fileSplit = (FileSplit)context.getInputSplit();
// String filename = fileSplit.getPath().getName();

        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            String[] result = str.toString().split(";");
            int n = result.length;

            String titleId = result[0];
            String directorId = result[1];          

            if (!titleId.equals("\\N") && !directorId.equals("\\N")) {
                String  midKey = titleId + " " + directorId;
                word.set(midKey);
                context.write(word, one);
            }

        }
    }
  }

  public static class QueryReducer1
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String actorName = "";
      int count = 0;
      
      for (Text val : values) {
        count++;
        String value = val.toString();
        char myChar = value.charAt(0);

        if (!Character.isDigit(myChar)) {
            actorName = value;
        }
      }

      if (count >= 2) {

        result.set(actorName);
        
        context.write(key, result);
      }
    }
  }

  public static class IntermediateMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            word.set(str);
            context.write(word, one);
        }
    }
  }

  public static class QueryReducer2
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      if (sum >= 100) {
          result.set(sum);
          context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    
    if (args.length != 3) {
        System.err.println("Usage : <inputlocation>  <inputlocation>  <outputlocation> ");
            System.exit(0);
    }

    String source1 = args[0];
    String source2 = args[1];
    String dest = args[2];

    Path p1 = new Path(source1);
    Path p2 = new Path(source2);
    Path out = new Path(dest);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(QueryIMDB.class);

    MultipleInputs.addInputPath(job, p1, TextInputFormat.class,
                MapperActors.class);
    MultipleInputs.addInputPath(job, p2, TextInputFormat.class,
                MapperDirectors.class);

    // job.setMapperClass(MapperActors.class);
    // job.setCombinerClass(QueryReducer1.class);
    job.setReducerClass(QueryReducer1.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // FileInputFormat.addInputPath(job, p1);
    // FileInputFormat.addInputPath(job, p2);
    Path temppath = new Path("/user/hduser/tmpoutput");
    if (fs.exists(temppath))
        fs.delete(temppath, true);

    FileOutputFormat.setOutputPath(job, temppath);
    job.waitForCompletion(true);

    // another mapper-reducer job
    Job job2 = Job.getInstance(conf, "word count second part");
    job2.setJarByClass(QueryIMDB.class);
    job2.setMapperClass(IntermediateMapper.class);
    job2.setCombinerClass(QueryReducer2.class);
    job2.setReducerClass(QueryReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    job2.setInputFormatClass(KeyValueTextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
    //job2.setNumReduceTasks(7);
    //FileInputFormat.setMaxInputSplitSize(job2, 67108864);
    FileInputFormat.addInputPath(job2, temppath);

    if (fs.exists(out))
        fs.delete(out, true);
    FileOutputFormat.setOutputPath(job2, out);

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
