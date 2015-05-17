
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Chainmapper2 extends Mapper<LongWritable,Text,Text,IntWritable>{
	public void map(Text key,IntWritable value,Context context ) throws IOException,InterruptedException
	{
		String myval=key.toString();
		
		if(myval.matches("hi") || myval.matches("hello")   )
		{
			context.write(new Text(myval),value);
		
		}
	}
}
