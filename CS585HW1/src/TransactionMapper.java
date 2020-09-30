
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//public class TransactionMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
//	private static String separator;
//	private static String commonSeparator;
//	private static String FILE_TAG = "F2";
//
//	public void setup(Context context) {
//		Configuration configuration = context.getConfiguration();
//		// Retrieving the file separator from context for file2.
//		separator = configuration.get("Separator.File2");
//		// Retrieving the file separator from context for writing the data to reducer.
//		commonSeparator = configuration.get("Separator.Common");
//	}
//
//	@Override
//	public void map(LongWritable rowKey, Text value, Context context) throws IOException, InterruptedException {
//		String[] values = value.toString().split(separator);
//		StringBuilder stringBuilder = new StringBuilder();
//		for (int index = 1; index < values.length; index++) {
//			stringBuilder.append(values[index] + commonSeparator);
//		}
//		if (values[0] != null && !"NULL".equalsIgnoreCase(values[0])) {
//			context.write(new LongWritable(Long.parseLong(values[0])),
//					new Text(FILE_TAG + commonSeparator + stringBuilder.toString()));
//		}
//	}
//}

public class TransactionMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String record = value.toString();
		String[] parts = record.split(",");
		context.write(new Text(parts[1]), new Text(parts[2]));
	}
}
