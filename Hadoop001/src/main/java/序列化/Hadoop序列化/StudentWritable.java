package 序列化.Hadoop序列化;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class StudentWritable implements Writable {
	private long stuid;
	private String name;
	private int age;	

	public long getStuid() {
		return stuid;
	}

	public void setStuid(long stuid) {
		this.stuid = stuid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

//	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(stuid);
		out.writeUTF(name);
		out.writeInt(age);
	}

//	@Override
	public void readFields(DataInput in) throws IOException {
		stuid = in.readLong();
		name = in.readUTF();
		age = in.readInt();		
	}

	public static void main(String[] args) throws IOException {
		StudentWritable student = new StudentWritable();
		student.setStuid(201699999);
		student.setName("jianghy");
		student.setAge(22);
		
		//将类对象序列化到文件
		FileOutputStream fileOut = new FileOutputStream("StudentWritable.ser");
		DataOutputStream dataOut= new DataOutputStream(fileOut);
		student.write(dataOut);	
		fileOut.close();
		dataOut.close();
		
		//从文件中反序列化重建类对象
		FileInputStream fileIn = new FileInputStream("StudentWritable.ser");
		DataInputStream dataIn = new DataInputStream(fileIn);
		StudentWritable newStudent = new StudentWritable();
		newStudent.readFields(dataIn);
		fileIn.close();
		dataIn.close();
		
		//输出反序列化类对象内容
		System.out.println("stuid : " + newStudent.getStuid());
		System.out.println("name : " + newStudent.getName());
		System.out.println("age : " + newStudent.getAge());		
	}
	
}
