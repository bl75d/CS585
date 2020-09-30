import java.util.Random;
import java.lang.StringBuilder;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;

public class Customer {
	int ID;
	String Name;
	int Age;
	String Gender;
	int CountryCode;
	float Salary;
	String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	
	public void newcustomer(int id) {
		ID=id;
		setName();
		setAge();
		setCountryCode();
		setGender();
		setSalary();
		
		
	}
	public void setName() {
		StringBuilder salt = new StringBuilder();
		Random r = new Random();
		int lth=10+r.nextInt(10);
		Random rnd = new Random();
        while (salt.length() < lth) { // length of the random string.
            int index = (int) (rnd.nextFloat() * alphabet.length());
            salt.append(alphabet.charAt(index));
        }
        Name = salt.toString();
//        System.out.println(Name);
	}
	public void setAge() {
		Random r = new Random();
		Age=10+r.nextInt(60);
	}
	
	public void setGender() {
		Random r = new Random();
		if(r.nextInt(2)==0) {
			Gender="male";
		}else {
			Gender="female";
		}
	}
	public void setCountryCode() {
		Random r = new Random();
		CountryCode=1+r.nextInt(10);
	}
	public void setSalary() {
		int max=10000;
		int min=100;
		Random r = new Random();
		Salary=r.nextFloat()* (max - min) + min;
//		System.out.println(Salary);
	}
	
	public static void main(String[] args) throws IOException {
		FileWriter fw=new FileWriter("Customer.txt");
		for(int i=0;i<50000;i++) {
			Customer addCustomer=new Customer();
			addCustomer.newcustomer(i+1);
			String dt=addCustomer.ID+","+addCustomer.Name+","+addCustomer.Age+","+addCustomer.Gender+","+addCustomer.CountryCode+","+addCustomer.Salary;
			fw.write(dt+"\n");
			}
		fw.close();
	}

}
