package 序列化.java序列化;

import 序列化.java序列化.Employee;

import java.io.*;

public class SerializeDemo {
	public static void main(String[] args) {
		Employee e = new Employee();
		e.name = "jianghy";
		e.address = "SSDUT Inovation and Practice Base";
		e.SSN = 112233;
		e.number = 423;
		try {
			FileOutputStream fileOut = new FileOutputStream("F:\\employee.ser");
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(e);
			out.close();
			fileOut.close();
			System.out.printf("Serialized data is saved in F:\\employee.ser");
		} catch (IOException i) {
			i.printStackTrace();
		}
	}
}
