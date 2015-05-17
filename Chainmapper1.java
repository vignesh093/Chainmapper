
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Chainmapper1 extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(LongWritable key,Text value,Context context ) throws IOException,InterruptedException
	{
		String myval=value.toString();
		String[] s=myval.split(",");
		if(s[0].matches("hi") || s[0].matches("hello") || s[0].matches("pig")  )
		{
			context.write(new Text(s[0]),new IntWritable(Integer.parseInt(s[1])));
		
		}
	}
}
