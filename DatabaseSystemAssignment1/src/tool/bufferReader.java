package tool;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class bufferReader {
	public static void main(String[] args) throws UnsupportedEncodingException 
	{
	    ByteBuffer bb = ByteBuffer.allocate(500);
	    printBufferInfo(bb);
	    for (int i = 50; i < 58; i++) {
	        bb.put((byte) i);
	      }
	    bb.put("Content of the String".getBytes("utf-8"));
		printBufferInfo(bb);


	}
	
	  public static void printBufferInfo(ByteBuffer bb) {
		    int limit = bb.limit();
		    System.out.println("Position =  " + bb.position() + ", Limit   = " + limit);
		    for (int i = 0; i < limit; i++) {
		      System.out.print(bb.get(i) + "  ");
		    }
		    System.out.println();
		  }

		public static String intHexMaker(String str) {
			char[] byteData  = str.toCharArray();
			String result = "";
				for(int i =0;i<byteData.length;i++) {
					//remove " " later
					String temp = "000" + Integer.toBinaryString(Character.getNumericValue(byteData[i]))+" ";
					if(temp.length()>=5) {
						temp = temp.substring(temp.length()-5,temp.length());
					}
					result += temp;
				}
			return result;
		}

}
