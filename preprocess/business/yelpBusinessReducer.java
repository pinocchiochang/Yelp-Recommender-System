import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class yelpBusinessReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for(Text t: values) {
			String tt = t.toString();
			if(!tt.contains(",")) continue;
			String[] ss = tt.split(",");
			int size = ss.length;
			if(size >= 2) {
				String textContent = "Recommend!";
				context.write(key, new Text(textContent));
			}
		}	
	}
}
