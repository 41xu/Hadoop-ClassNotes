package 序列化.java序列化;

import java.io.*;

public class DeserializeDemo {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Employee e = null;
		FileInputStream fileIn = new FileInputStream("F:\\employee.ser");
		ObjectInputStream in = new ObjectInputStream(fileIn);
		e = (Employee) in.readObject();	//反序列化对象
		in.close();
		fileIn.close();

		System.out.println("Deserialized Employee...");
		System.out.println("Name: " + e.name);
		System.out.println("Address: " + e.address);
		System.out.println("SSN: " + e.SSN);
		System.out.println("Number: " + e.number);
		e.mailCheck();
	}
}