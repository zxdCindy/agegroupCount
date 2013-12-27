package question1;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Get user-tag counts and filter out counts less than 5
 * @author cindyzhang
 *
 */
public class UserTagCount {
	
	public static class UserTagMapper 
		extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		Text outputKey = new Text();
		LongWritable ONE = new LongWritable(1);		
		String userId = null; String tag = null;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
			Map<String, String> parsed = MyUtility.transformXmlToMap(value.toString());
			if(parsed.containsKey("OwnerUserId")){
				userId = parsed.get("OwnerUserId");
				if(parsed.get("Tags") != null){
					String[] tags = parsed.get("Tags").split("&lt;|&gt;");
					for(int i=1;i<tags.length;i=i+2){
						tag = tags[i];
						outputKey.set(userId + "\t" + tag);
						context.write(outputKey, ONE);
					}
				}				
			}
		}
		
	}
	
	public static class UserTagReducer
		extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable outputValue = new LongWritable();
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException{
			long sum = 0l;
			for(LongWritable value: values){
				sum += value.get();
			}
			if(sum > 5){
				outputValue.set(sum);
				context.write(key, outputValue);
			}
		}
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) 
			throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "UserTagCount");
		job.setJarByClass(UserTagCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setMapperClass(UserTagMapper.class);
		job.setReducerClass(UserTagReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);

	}

}
