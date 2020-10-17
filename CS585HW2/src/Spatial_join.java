
//https://www.edureka.co/blog/mapreduce-example-reduce-side-join/
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Spatial_join {

	static class Window {
		int x1;
		int y1;
		int x2;
		int y2;

		public void set_window(int a, int b, int c, int d) {
			x1 = a;
			y1 = b;
			x2 = c;
			y2 = d;
		}
	}

	public static class pointmapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Window window = new Window();
			//define your window size here and below
			window.set_window(1, 3, 1000, 2000);

			String data = value.toString();
			String[] pnt = data.split("\\(|,|\\)");
			int x = Integer.parseInt(pnt[1]);
			int y = Integer.parseInt(pnt[2]);
			if (x <= window.x2 & x >= window.x1 & y <= window.y2 & y >= window.y1) {
				// System.out.println(value);

				context.write(new Text("W"), new Text(value));
			}
		}
	}

	public static class rectanglemapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			Window window = new Window();
			//define your window size here and below
			window.set_window(1, 3, 1000, 2000);
			int w_x1 = window.x1;
			int w_y1 = window.y1;
			int w_x2 = window.x2;
			int w_y2 = window.y2;

			String data = value.toString();
			String[] rec = data.split("\\<|,|\\>");
			int r_x1 = Integer.parseInt(rec[1]);
			int r_y1 = Integer.parseInt(rec[2]);
			int r_x2 = Integer.parseInt(rec[4]) + r_x1;
			int r_y2 = Integer.parseInt(rec[3]) + r_y1;
			if (!(r_x1 > w_x2 || r_x2 < w_x1 || r_y1 > w_y2 || r_y2 < w_y1)) {
				context.write(new Text("W"), new Text(value));
			}
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if (key.toString().equals("W")) {
				List<String> Pnt = new ArrayList<String>();
				List<String> Rec = new ArrayList<String>();
				for (Text t : values) {
					String[] dt = t.toString().split("\\(|,|\\)|\\<|\\>");
					if (dt.length == 3) {
						Pnt.add(t.toString());
					} else if (dt.length == 5) {
						Rec.add(t.toString());
					}
				}
				System.out.println("**********");
				for (int i = 0; i < Pnt.size(); i++) {
					String[] point = Pnt.get(i).split("\\(|,|\\)");
					int px = Integer.parseInt(point[1]);
					int py = Integer.parseInt(point[2]);

					for (int j = 0; j < Rec.size(); j++) {
						String[] rect = Rec.get(j).split("\\<|,|\\>");
						int r_x1 = Integer.parseInt(rect[1]);
						int r_y1 = Integer.parseInt(rect[2]);
						int r_x2 = Integer.parseInt(rect[4]) + r_x1;
						int r_y2 = Integer.parseInt(rect[3]) + r_y1;
						if (r_x1 <= px && r_x2 >= px && r_y1 <= py && r_y2 >= py) {
							System.out.println(Rec.get(j));
							System.out.println(Pnt.get(i));
							context.write(new Text(Rec.get(j)), new Text(Pnt.get(i)));
						}
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Spactial side join");
		job.setJarByClass(Spatial_join.class);

		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Point.txt"),
				TextInputFormat.class, pointmapper.class);
		MultipleInputs.addInputPath(job, new Path("/Users/merqurius/Workspace/CS585Data/Rec.txt"),
				TextInputFormat.class, rectanglemapper.class);
		Path outputPath = new Path("/Users/merqurius/Workspace/CS585Data/out/");
		FileOutputFormat.setOutputPath(job, outputPath);
//		outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
