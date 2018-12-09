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


public class QueryIMDB {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
FileSplit fileSplit = (FileSplit)context.getInputSplit();
String filename = fileSplit.getPath().getName();
        String str = value.toString();
        String[] result = str.toString().split(";");
        int n = result.length;

        String year = result[n-2];

        if (!year.equals("\\N")) {
            if (Integer.parseInt(year) >= 2000) {
                String genreList = result[n-1];
                
                String genreArray[] = genreList.split(",");
                for(String genre: genreArray) {
                    if (!genre.equals("\\N")) {

                        String intermidiateKey = genre + " " + year;
                        word.set(intermidiateKey);
                        context.write(word, one);
                    }
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

      String strArr[] = key.toString().split(" ");
      if (strArr.length > 1) {
          String year = strArr[1];
          String genre = strArr[0];
    
          String newKey = year + "," + genre + ",";
          key.set(newKey);
      }
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Path out = new Path(args[1]);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(QueryIMDB.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    if (fs.exists(out))
        fs.delete(out, true);
    FileOutputFormat.setOutputPath(job, out);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
