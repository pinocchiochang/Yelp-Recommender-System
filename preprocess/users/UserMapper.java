import java.util.*;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class UserMapper
extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
	String line = value.toString();
	//int s=line.indexOf("user_id");
	int pos=line.indexOf("average_stars")+16;
	String star="";
	while(pos<line.length()){
		if(line.charAt(pos)!=','){
		star+=line.charAt(pos);
		pos++;		
		}else{
		break;
		}
	}	
	//String user_id=line.substring(s+11,s+33);
	double val=Double.parseDouble(star);
	context.write(new IntWritable((int)val), new IntWritable(1));
}
}
