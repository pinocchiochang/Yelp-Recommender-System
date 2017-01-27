import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class UserReducer
extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
@Override
public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
throws IOException, InterruptedException {
int totalCount = 0;
for (IntWritable value : values) {
totalCount += value.get();
}
context.write(key, new IntWritable(totalCount));
}
}

