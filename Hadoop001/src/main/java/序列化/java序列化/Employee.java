package 序列化.java序列化;

public class Employee implements java.io.Serializable {
	private static final long serialVersionUID = 1L;	//用来验证版本一致性

	public String name;
	public String address;
	public transient int SSN;		//用trainsient定义的字段不会被序列化
	public int number;

	public void mailCheck() {
		System.out.println("Mailing a check to " + name + " " + address);
	}

}
