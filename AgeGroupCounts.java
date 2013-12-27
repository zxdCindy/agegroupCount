package question1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import question1.UserTagCount.UserTagMapper;
import question1.UserTagCount.UserTagReducer;

/**
 * Get the age group count
 * @author cindyzhang
 *
 */
public class AgeGroupCounts {
	
	public static class AgeGroupMapper 
		extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text outputKey = new Text();
		LongWritable outputValue = new LongWritable();
		private HashMap<String, Integer> userAge = new HashMap<String, Integer>();
		
		/**
		 * Use distributed cash to do the inner join
		 */
		@Override
		public void setup(Context context) throws IOException{
			Path[] files = DistributedCache.getLocalCacheFiles
					(context.getConfiguration());
			for(Path p: files){
				BufferedReader rdr = new BufferedReader
						(new InputStreamReader(new GZIPInputStream
								(new FileInputStream(new File(p.toString())))));
				String line = null;
				while((line = rdr.readLine()) != null){
					Map<String, String> parsed = MyUtility.transformXmlToMap(line);
					if(parsed.containsKey("Id") && parsed.containsKey("Age")){
						String userId = parsed.get("Id");
						Integer age = Integer.valueOf(parsed.get("Age"));
						userAge.put(userId, age);
					}
				}
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			String[] tagCount = value.toString().split("\t");
			String userId = tagCount[0];
			String tag = tagCount[1];
			long count = Long.valueOf(tagCount[2]);
			Integer age = userAge.get(userId);
			if(age != null){
				if(age >=0 && age <=20){
					outputKey.set("0-20\t"+tag);
					outputValue.set(count);					
				}
				else if(age >=21 && age <=30){
					outputKey.set("21-30\t"+tag);
					outputValue.set(count);
				}
				else if(age >=31 && age <= 40){
					outputKey.set("31-40\t"+tag);
					outputValue.set(count);
				}
				else if(age >=41 && age <= 100){
					outputKey.set("41-100\t"+tag);
					outputValue.set(count);
				}
				context.write(outputKey, outputValue);
			}
		}		
	}
	
	public static class AgeGroupReducer extends 
		Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable outputValue = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException{
			long sum = 0l;
			for(LongWritable num: values){
				sum += num.get();
			}
			outputValue.set(sum);
			context.write(key, outputValue);
		}
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AgeGroupCount");
		job.setJarByClass(AgeGroupCounts.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(AgeGroupMapper.class);
		job.setReducerClass(AgeGroupReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		DistributedCache.addCacheFile(
				FileSystem.get(conf).makeQualified(new Path(args[2]))
						.toUri(), job.getConfiguration());
		
		job.waitForCompletion(true);

	}

}
