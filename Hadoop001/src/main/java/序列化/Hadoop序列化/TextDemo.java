package 序列化.Hadoop序列化;

import org.apache.hadoop.io.Text;

public class TextDemo {	
	
	public static void main(String args[]) {
		String s1 = "hello world";
		Text t1 = new Text();				//定义Text
		t1.set(s1);							//赋值
		//Text t1 = new Text(s1);
		
		System.out.println("s1=" + s1);		//字符串内容
		System.out.println("t1=" + t1.toString());
		
		System.out.println(s1.length());	//字符串长度
		System.out.println(t1.getLength());
		
		System.out.println(s1.indexOf("ll"));	//查找某个字符(串)的下标
		System.out.println(t1.find("ll"));
		
		System.out.println(s1.charAt(1));		//当前位置对应的字符（char类型）
		System.out.println((char)t1.charAt(1));	//charAt返回当前位置字符对应的Unicode编码的位置（int类型）
		
		byte[] bytes = "hello hadoop".getBytes();
		t1.append(bytes, 2, 6);					//追加字节数组
		System.out.println("t1=" + t1.toString());
		
		t1.clear();		//清除内容
		System.out.println("t1=" + t1.toString());		
	}
	
}
