import java.util.Random;
import java.lang.StringBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

public class Transaction {
	int TransID;
	int CustID;
	float TransTotal;
	int TransNumItems;
	String TransDesc;
	
	String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	
	public void newtransaction(int id) {
		TransID=id;
		setCustID();
		setTransTotal();
		setTransNumItems();
		setTransDesc();
		
		
	}

	public void setCustID() {
		Random r = new Random();
		CustID=1+r.nextInt(50000);
	}
	
	public void setTransTotal() {
		Random r = new Random();
		int max=1000;
		int min=10;
		TransTotal=r.nextFloat()* (max - min) + min;
	}
	public void setTransNumItems() {
		Random r = new Random();
		TransNumItems=1+r.nextInt(10);
	}
	
	public void setTransDesc() {
		StringBuilder salt = new StringBuilder();
		Random r = new Random();
		int lth=20+r.nextInt(30);
		Random rnd = new Random();
        while (salt.length() < lth) { // length of the random string.
            int index = (int) (rnd.nextFloat() * alphabet.length());
            salt.append(alphabet.charAt(index));
        }
        TransDesc = salt.toString();
	}
	
	public static void main(String[] args) throws IOException {
		FileWriter fw=new FileWriter("Transaction.txt");
		for(int i=0;i<5000000;i++) {
			Transaction addTransactionr=new Transaction();
			addTransactionr.newtransaction(i+1);
			String dt=addTransactionr.TransID+","+addTransactionr.CustID+","+addTransactionr.TransTotal+","+addTransactionr.TransNumItems+","+addTransactionr.TransDesc;
			fw.write(dt+"\n");
			}
		fw.close();
	}

}
