import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class reviewMapper
extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
	//if (line.contains("user_id") && line.contains("stars")){
	//int s=line.indexOf("user_id");
	//String stars=line.substring(line.indexOf("stars")+8,
	//line.indexOf("starts")+9);
	
	//String user_id=line.substring(s+11,s+33);
		String business_id=line.substring(line.indexOf("business_id")+15,
		line.indexOf("business_id")+37);
		context.write(new Text(business_id), 
		new IntWritable(1) );
	//}
	}
}
