package hw3;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class CreateM12 {

	int x;
	int y;
	int value;
	
	public void create_data(int a,int b) {
		Random r = new Random();
		x=a;
		y=b;
		value=1+r.nextInt(20);
	}
	
	public static void main(String[] args) throws IOException {
		FileWriter fw=new FileWriter("M1.txt");
		FileWriter fw2=new FileWriter("M2.txt");
		for(int i=0;i<500;i++) {
			for(int j=0;j<500;j++){
				CreateM12 M=new CreateM12();
				M.create_data(i,j);
				String dt=M.x+","+M.y+","+M.value;
//				System.out.println(dt);
				fw.write(dt+"\n");
				
				CreateM12 M2=new CreateM12();
				M2.create_data(i,j);
				String dt2=M.x+","+M.y+","+M.value;
//				System.out.println(dt2);
				fw2.write(dt2+"\n");
				}
			}
		fw.close();
		fw2.close();

		
	}
	
}
