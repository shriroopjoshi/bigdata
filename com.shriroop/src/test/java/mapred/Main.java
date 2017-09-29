package mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import classes.FriendsPair;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length < 2) {
			System.err.println("USAGE: jar <input-file> <output-file>");
			System.exit(1);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "common friends");
		job.setJarByClass(Main.class);
		
		job.setMapperClass(mapred.FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		
		job.setOutputKeyClass(FriendsPair.class);
		job.setOutputValueClass(ArrayWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean success = job.waitForCompletion(true);
		if(success)
			System.exit(0);
		else
			System.exit(-1);
	}

}
