import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PairBuilder {
	//final static int SIZE = 10000;
	public static class ReaderMapper
	extends Mapper<Object, Text, Text, Text>{
		private static List<String> listCache =  new LinkedList<String>();
		protected void setup(Context context) throws IOException, InterruptedException {
			Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			this.readFile(paths[0]);
		}

		private void readFile(Path filePath) {
			try{
				BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
				String line = null;
				while((line = bufferedReader.readLine()) != null) {
					listCache.add(line);
				}
				bufferedReader.close();
			} catch(IOException ex) {
				System.err.println("Exception while reading");
			}
		}
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			for (String wordCache: listCache) {
				if ((value.toString().length()-1)*20/17 >= wordCache.length()-1
						&& wordCache.length()-1 >= 0.85*value.toString().length()) {
					context.write(value, new Text(wordCache));
					//System.out.println(wordCache+"\t"+value.toString());
				}
			}
		}
	}

	public static class JoinReducer
	extends Reducer<Text, Iterable<Text>, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			Text v = values.iterator().next();
			context.write(key, v);
		}
	}

  public static void main(String[] args) throws Exception {
      Runtime.getRuntime().exec("rm ./Pairs -R"); // DEBUG: Removing output on the fly

      Date date; long startTime, endTime; // for recording starting and end time of job
      date = new Date(); startTime = date.getTime(); // starting timer

      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "pair maker");
      job.setJarByClass(PairBuilder.class);
      job.setReducerClass(JoinReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(ReaderMapper.class);
      DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
      FileInputFormat.addInputPath(job, new Path(args[1]));
      //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReaderMapper.class);
      //  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReaderMapper2.class);
    
      FileOutputFormat.setOutputPath(job, new Path("./Pairs"));
      
      job.waitForCompletion(true);
      endTime = date.getTime();
      date = new Date(); endTime = date.getTime(); //end timer
      System.out.println("Total Time (in milliseconds) = "+ (endTime-startTime));  }
}
