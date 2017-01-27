import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class yelpBusinessMapper
        extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] attributes = {"good for dancing", "parking", "dinner", "lunch", "breakfast", "brunch", "desert", "alcohol", "Outdoor Seating", "has TV"};
        
        int start = line.indexOf("name")+8;
		int end = line.indexOf(",", start)-1;
		String name = line.substring(start, end);
		StringBuilder sb = new StringBuilder();

		start = line.indexOf("attributes")+12;
		end = line.indexOf("type")-4;
		String attrs = line.substring(start, end).toLowerCase();
		for(int i=0; i<attributes.length; ++i) {
			String attr = attributes[i].toLowerCase();
			if(attrs.contains(attr)) {
				int l = attrs.indexOf(attr);
				int r = attrs.indexOf(",", l);
				if(attrs.substring(l, r).contains("true")) {
					sb.append(attr);
					sb.append(",");
				}
			}
		}
		String businessAttrs = sb.toString();
		context.write(new Text(name), new Text(businessAttrs));
    }
}
