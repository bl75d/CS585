import java.io.IOException;
import java.lang.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.xbill.DNS.SIG0;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OutlierDetection {

	static class Boundary {
		int x_bot = 0;
		int y_bot = 0;
		int x_top = 10000;
		int y_top = 10000;
	}

	static class Segment {
		int size = 100;
		int x1;
		int x2;
		int y1;
		int y2;
		int r = 100;
		int k = 15;

		public void set_segment(int x_left, int x_right, int y_bot, int y_top) {
			x1 = x_left;
			y1 = y_bot;
			x2 = x_right;
			y2 = y_top;
		}

	}

	public static class Pointmapper extends Mapper<Object, Text, Text, Text> {
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Segment seg = new Segment();
			Boundary bnd = new Boundary();
			// define your window size here and below

			String data = value.toString();
			String[] pnt = data.split("\\(|,|\\)");

			int x = Integer.parseInt(pnt[1]);
			int y = Integer.parseInt(pnt[2]);

			int row = bnd.y_top / seg.size;
			int col = bnd.x_top / seg.size;
			int x_l = x - x % seg.size;
			int x_r = x + seg.size - x % seg.size;
			int y_b = y - y % seg.size;
			int y_t = y + seg.size - y % seg.size;
			seg.set_segment(x_l, x_r, y_b, y_t);

			if (x <= x_l + seg.r && x_l > bnd.x_bot) {
				seg.set_segment(x_l - seg.size, x_r - seg.size, y_b, y_t);
				String segment1 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
				context.write(new Text(segment1), new Text(value));

				if (y <= y_b + seg.r && y_b > 0) {
					seg.set_segment(x_l - seg.size, x_r - seg.size, y_b - seg.size, y_t - seg.size);
					String segment2 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment2), new Text(value));

					seg.set_segment(x_l, x_r, y_b - seg.size, y_t - seg.size);
					String segment3 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment3), new Text(value));

				} else if (y >= y_t - seg.r && y_t < bnd.y_top) {
					seg.set_segment(x_l - seg.size, x_r - seg.size, y_b + seg.size, y_t + seg.size);
					String segment4 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment4), new Text(value));

					seg.set_segment(x_l, x_r, y_b + seg.size, y_t + seg.size);
					String segment5 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment5), new Text(value));
				}
			} else if (x >= x_r - seg.r && x_r < bnd.x_top) {
				seg.set_segment(x_l + seg.size, x_r + seg.size, y_b, y_t);
				String segment6 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
				context.write(new Text(segment6), new Text(value));

				if (y <= y_b + seg.r && y_b > 0) {
					seg.set_segment(x_l, x_r, y_b - seg.size, y_t - seg.size);
					String segment7 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment7), new Text(value));

					seg.set_segment(x_l + seg.size, x_r + seg.size, y_b - seg.size, y_t - seg.size);
					String segment8 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment8), new Text(value));

				} else if (y >= y_t - seg.r && y_t < bnd.y_top) {
					seg.set_segment(x_l, x_r, y_b + seg.size, y_t + seg.size);
					String segment9 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment9), new Text(value));

					seg.set_segment(x_l + seg.size, x_r + seg.size, y_b + seg.size, y_t + seg.size);
					String segment10 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
					context.write(new Text(segment10), new Text(value));

				}

			} else if (y <= y_b + seg.r && y_b > bnd.y_bot) {
				seg.set_segment(x_l, x_r, y_b - seg.size, y_t - seg.size);
				String segment11 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
				context.write(new Text(segment11), new Text(value));

			} else if (y >= y_t - seg.r && y_t < bnd.y_top) {
				seg.set_segment(x_l, x_r, y_b + seg.size, y_t + seg.size);
				String segment12 = "<" + seg.x1 + "," + seg.x2 + "," + seg.y1 + "," + seg.y2 + ">";
				context.write(new Text(segment12), new Text(value));

			}
		}
	}

	public static class OtlReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Segment seg = new Segment();
			String[] segment = key.toString().split(",|\\<|\\>");
			int x_left = Integer.parseInt(segment[1]);
			int x_right = Integer.parseInt(segment[2]);
			int y_bot = Integer.parseInt(segment[3]);
			int y_top = Integer.parseInt(segment[4]);
			seg.set_segment(x_left, x_right, y_bot, y_top);

			for (Text t : values) {
				System.out.println(t);
				String[] pnt = t.toString().split("\\(|,|\\)");
				int x = Integer.parseInt(pnt[1]);
				int y = Integer.parseInt(pnt[2]);
				if (x >= x_left && x <= x_right && y >= y_bot && y <= y_top) {
					int count = 0;
					for (Text p : values) {
						String[] pt = t.toString().split("\\(|,|\\)");
						int x_n = Integer.parseInt(pt[1]);
						int y_n = Integer.parseInt(pt[2]);
						if (Math.sqrt((x - x_n) ^ 2 + (y - y_n) ^ 2) < seg.r) {
							count++;
						}
					}
					if (count < (seg.k + 1)) {
						context.write(new Text("Outlier"), new Text(t));

					}

				}

			}

		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Outliner Detection");

		job.setJarByClass(OutlierDetection.class);

		job.setMapperClass(Pointmapper.class);
		job.setReducerClass(OtlReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		Path outputPath = new Path("/Users/merqurius/Workspace/CS585Data/out/");

		FileInputFormat.setInputPaths(job, new Path("/Users/merqurius/Workspace/CS585Data/Point.txt"));
		FileOutputFormat.setOutputPath(job, outputPath);
		// outputPath.getFileSystem(conf).delete(outputPath);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
