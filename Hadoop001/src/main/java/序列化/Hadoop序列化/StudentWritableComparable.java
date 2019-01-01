package 序列化.Hadoop序列化;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class StudentWritableComparable implements WritableComparable<StudentWritableComparable> {
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
	
//	@Override
	public int compareTo(StudentWritableComparable o) {
		//按年龄大小比较
		if (this.age < o.age)
			return -1;
		else if (this.age > o.age)
			return 1;
		else
			return 0;
	}
	
	public static void main(String[] args) {
		StudentWritableComparable s1 = new StudentWritableComparable();
		StudentWritableComparable s2 = new StudentWritableComparable();
		s1.setStuid(201611111);
		s1.setAge(20);
		s1.setName("wang");
		s2.setStuid(201622222);
		s2.setAge(20);
		s2.setName("liu");
		
		if ( s1.compareTo(s2) == 0 )
			System.out.println("s1 equal s2");
		else if (s1.compareTo(s2) < 0 )
			System.out.println("s1 less than s2");
		else
			System.out.println("s1 great than s2");
	}

}
