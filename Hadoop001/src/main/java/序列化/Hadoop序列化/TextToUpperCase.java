package 序列化.Hadoop序列化;

import org.apache.hadoop.io.Text;

public class TextToUpperCase {

	public static void main(String[] args) {
		Text text = new Text("HDFS is hadoop distribute filesystem");	//初始化Text
		String[] strs = text.toString().split(" ");			//将Text转成String
		
		//将首字母转换为大写并重新拼接
		for(int i=0; i<strs.length; i++) {
//			System.out.println(strs[i]);
			strs[i] = strs[i].substring(0, 1).toUpperCase() + strs[i].substring(1);
		}
		StringBuffer sb = new StringBuffer();
		for(String s : strs) {
			sb.append(s).append(" ");
		}
//		System.out.println(sb.toString());
		text.set(sb.toString());	//重置Text内容
		System.out.println(text);
	}

}
