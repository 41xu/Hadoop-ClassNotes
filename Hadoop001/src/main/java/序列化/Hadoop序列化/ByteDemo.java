package 序列化.Hadoop序列化;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

public class ByteDemo {

	public static void main(String[] args) throws IOException {
		ByteWritable b1 = new ByteWritable((byte)'a');
		ByteWritable b2 = new ByteWritable((byte)97);	//byte取值范围 -128~127	
		System.out.println(b1);
		System.out.println(b2);
		
		byte[] bytes = "abcd1234".getBytes();
		BytesWritable b3 = new BytesWritable(bytes);
		System.out.println(b3);
		
		FileOutputStream fileOut = new FileOutputStream("ByteWritable.ser");
		DataOutputStream dataOut= new DataOutputStream(fileOut);
		b1.write(dataOut);	
		b2.write(dataOut);
		b3.write(dataOut);
	}

}
