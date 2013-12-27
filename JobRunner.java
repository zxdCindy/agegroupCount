package question1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import question1.AgeGroupCounts.AgeGroupMapper;
import question1.AgeGroupCounts.AgeGroupReducer;
import question1.UserTagCount.UserTagMapper;
import question1.UserTagCount.UserTagReducer;

/**
 * Use Job chaining to run two jobs
 * @author cindyzhang
 *
 */
public class JobRunner {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Path postInput = new Path(args[0]);
		Path userInput = new Path(args[1]);
		Path userTagOutput = new Path(args[2]);
		Path ageGroupOutput = new Path(args[3]);
		
		Job userTagJob = new Job(conf, "UserTagCount");
		userTagJob.setJarByClass(UserTagCount.class);
		userTagJob.setOutputKeyClass(Text.class);
		userTagJob.setOutputValueClass(LongWritable.class);
		userTagJob.setMapperClass(UserTagMapper.class);
		userTagJob.setReducerClass(UserTagReducer.class);
		userTagJob.setInputFormatClass(TextInputFormat.class);
		userTagJob.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(userTagJob, postInput);
		FileOutputFormat.setOutputPath(userTagJob, userTagOutput);
		
		int code = userTagJob.waitForCompletion(true) ? 0 : 1;

		if(code == 0){
			Job ageGroupJob = new Job(conf, "AgeGroupCount");
			ageGroupJob.setJarByClass(AgeGroupCounts.class);
			ageGroupJob.setOutputKeyClass(Text.class);
			ageGroupJob.setOutputValueClass(LongWritable.class);
			ageGroupJob.setMapperClass(AgeGroupMapper.class);
			ageGroupJob.setReducerClass(AgeGroupReducer.class);
			ageGroupJob.setInputFormatClass(TextInputFormat.class);
			ageGroupJob.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(ageGroupJob, userTagOutput);
			FileOutputFormat.setOutputPath(ageGroupJob, ageGroupOutput);
			DistributedCache.addCacheFile(
					FileSystem.get(conf).makeQualified(userInput)
							.toUri(), ageGroupJob.getConfiguration());
			code = ageGroupJob.waitForCompletion(true) ? 0 : 1;
		}
		
		FileSystem.get(conf).delete(userTagOutput,true);
		System.exit(code);
	}

}
