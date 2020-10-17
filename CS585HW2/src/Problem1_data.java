import java.util.Random;
import java.io.FileWriter;
import java.io.IOException;

//These parameters can only generate 10000 data of points and rectangels, the size of output data file is around 100K
class Data {
	static class points {
		int x;
		int y;
		int boundry = 10000;

		public void create_point() {
			Random rp = new Random();
			x = rp.nextInt(boundry);
			y = rp.nextInt(boundry);
		}
	}

	static class rectangle {
		int bot_leftx;
		int bot_lefty;
		int w;
		int h;
		int boundry = 10000;
		int w_boundry = 5;
		int h_boundry = 20;

		public void create_rec() {
			Random rc = new Random();
			bot_leftx = rc.nextInt(boundry - w_boundry);
			bot_lefty = rc.nextInt(boundry - h_boundry) + h_boundry;
			w = rc.nextInt(w_boundry) + 1;
			h = rc.nextInt(h_boundry) + 1;
		}
	}
}

public class Problem1_data {
	public static void main(String[] args) throws IOException {
		//change n to enlarge the size of data file
//		int n = 100000;
		int n = 10000000;

		FileWriter pfw = new FileWriter("Point.txt");
		FileWriter rfw = new FileWriter("Rec.txt");
		for (int i = 0; i < n; i++) {
			Data.points pnt = new Data.points();
			pnt.create_point();
			Data.rectangle rec = new Data.rectangle();
			rec.create_rec();
			String pt = "(" + pnt.x + "," + pnt.y + ")";
			String rc = "<" + rec.bot_leftx + "," + rec.bot_lefty + "," + rec.w + "," + rec.h+">";
			pfw.write(pt + "\n");
			rfw.write(rc + "\n");
		}
		pfw.close();
		rfw.close();
	}

}
