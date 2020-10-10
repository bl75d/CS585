
//***************************
//https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class Query2 {
	public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[0]), new Text("CustName " + parts[1]));
		}
	}

	public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			String[] parts = record.split(",");
			context.write(new Text(parts[1]), new Text("TransTotal " + parts[2]));
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String name = "";
			if (key.toString().split(" ").length == 1) {
				
// This branch is for Combiner
				int count = 0;
				float total = 0;
				int flag = 0;
				String str="";
				String cust="Combine " + key.toString();
				for (Text t : values) {
					String parts[] = t.toString().split(" ");
					if (parts[0].equals("TransTotal")) {
						flag = 1;
						count++;
						total += Float.parseFloat(parts[1]);
					} else if (parts[0].equals("CustName")) {
						name = parts[1];
					}
				}
				if (flag == 1) {
					
					str = String.format("%d %f", count, total);
					context.write(new Text(cust), new Text("Combinedtrans " + str));
				} else {
					str = name.toString();
					context.write(new Text(cust), new Text("Combinename " + str));

				}
				
// This branch is for Reducer
			} else if (key.toString().split(" ").length == 2) {
				String keys[] = key.toString().split(" ");
				String id = keys[1];
				float cbntotal = 0;
				int cbncount = 0;
				for (Text t : values) {
					String parts[] = t.toString().split(" ");
					if (parts[0].equals("Combinedtrans")) {
						cbncount += Integer.parseInt(parts[1]);
						cbntotal += Float.parseFloat(parts[2]);
					} else if (parts[0].equals("Combinename")) {
						name = parts[1];
					}
				}
				String str = String.format("%d %f", cbncount, cbntotal);
				String cust = String.format("%s %s", id.toString(),name.toString());
				context.write(new Text(cust), new Text(str));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// Job job = Job.getInstance(conf, "Reduce-side join");
		Job job = new Job(conf, "Reduce-side join");

		job.setJarByClass(Query2.class);

		job.setCombinerClass(JoinReducer.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,
		// CustomerMapper.class);
		// MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,
		// TransactionMapper.class);

		MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Customer.txt"),
				TextInputFormat.class, CustomerMapper.class);
		MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Transaction.txt"),
				TextInputFormat.class, TransactionMapper.class);
		Path outputPath = new Path("/Users/merqurius/Workspace/CS585Data/out/");

		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);
	}
}
