import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
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



public class Score {
	private static int n = 2;
	public static void set_n(int n) {
		Score.n = n;
	}
	public static class ReaderMapper
	extends Mapper<Object, Text, Text, Text>{
		/*
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
		 */
		private static Text value_out = new Text();
		private static Text key_out = new Text();
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String value_s = value.toString();
			int x =value_s.indexOf("\t");
			String word1 = value_s.substring(0, x);
			String word2 = value_s.substring(x+1, value_s.length());
			Set<String> ngrams1 = new HashSet<String>();
			Set<String> ngrams2 = new HashSet<String>();
			
			for (int i=0; i<word1.length()-n+1; ++i) {
				ngrams1.add(word1.substring(i, i+n));
			}
			for (int i=0; i<word2.length()-n+1; ++i) {
				ngrams2.add(word2.substring(i, i+n));
			}
			
			ngrams1.retainAll(ngrams2);
			int intersection = ngrams1.size();
			if (intersection == 0)
				return;
			ngrams1.addAll(ngrams2);
			int total = ngrams1.size();
			double score = (double) intersection/total;

			if (score>=0.85 && score!=1) {
				key_out.set(word1);
				value_out.set(word2);
				context.write(key_out, value_out);
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
		  String s1_pathstring = args[0];
		  String s2_pathstring = args[1];
		  int n = Integer.parseInt(args[2]);
		  
		  Path output = new Path("Output");
		  Path output_tmp = new Path("Output.tmp");
		  
	      Runtime.getRuntime().exec("rm ./Output -R"); // DEBUG: Removing output on the fly

	      Date date; long startTime, pairBuildingTime, endTime; // for recording starting and end time of job
	      date = new Date(); startTime = date.getTime(); // starting timer

	      
	      Configuration conf = new Configuration();
	      FileSystem.get(conf).delete(output_tmp);
	      Job job = Job.getInstance(conf, "pair maker");
	      job.setJarByClass(PairBuilder.class);
	      job.setReducerClass(PairBuilder.JoinReducer.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(Text.class);
	      job.setMapperClass(PairBuilder.ReaderMapper.class);
	      DistributedCache.addCacheFile(new Path(s1_pathstring).toUri(), job.getConfiguration());
	      FileInputFormat.addInputPath(job, new Path(s2_pathstring));

	      FileOutputFormat.setOutputPath(job, output_tmp);
	      //job.setOutputFormatClass(SequenceFileOutputFormat.class);
	      
	      job.waitForCompletion(true);
	      date = new Date(); pairBuildingTime = date.getTime() - startTime; //end timer
	      
	      Score.set_n(n);
	      Configuration conf2 = new Configuration();
	      FileSystem.get(conf2).delete(output);

	      Job job2 = Job.getInstance(conf2, "score selectionner");
	      FileInputFormat.setInputPaths(job2, new Path("Output.tmp/part-r-00000"));
	      job2.setJarByClass(Score.class);
	      job2.setReducerClass(JoinReducer.class);
	      job2.setOutputKeyClass(Text.class);
	      job2.setOutputValueClass(Text.class);

	      job2.setMapperClass(Score.ReaderMapper.class);
	      //job2.setInputFormatClass(SequenceFileInputFormat.class);
	      //DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
	      //FileInputFormat.addInputPath(job2, new Path(args[0]));
	      //MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReaderMapper.class);
	      //  MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReaderMapper2.class);
	    
	      FileOutputFormat.setOutputPath(job2, output);
	      
	      job2.waitForCompletion(true);
	      endTime = date.getTime();
	      date = new Date(); endTime = date.getTime(); //end timer
	      System.out.println("Pair building Time (in milliseconds) = "+ (pairBuildingTime));  
	      System.out.println("Total Time (in milliseconds) = "+ (endTime-startTime));  
	      }
	}

