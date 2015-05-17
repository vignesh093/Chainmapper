
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Chainmapperdriver {
	public static void main(String args[]) throws Exception
	{
		if(args.length!=2)
			{
			System.err.println("Usage: Worddrivernewapi <input path> <output path>");
			System.exit(-1);
			}
		Configuration conf=new Configuration();
		Job job=new Job(conf,"Chainmapperexample");
		
		job.setJarByClass(Chainmapperdriver.class);
		job.setJobName("Chainmapperdriver");
		
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		Configuration chain1=new Configuration(false);
		ChainMapper.addMapper(job, Chainmapper1.class, LongWritable.class, Text.class, Text.class, IntWritable.class, chain1);
		
		job.setReducerClass(Myreducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
