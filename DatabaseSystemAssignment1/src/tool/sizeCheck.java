package tool;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

public class sizeCheck {
	public static void main(String[] args) throws UnsupportedEncodingException 
	{
	    ByteBuffer bb = ByteBuffer.allocate(500);
	    
	    bb.put(intByteArrayMaker(99999));
	    System.out.println(bb.position());

	    int[] size = {1,15,56,88,40,25,23,12,12,52};
	    int[] address = new int[10];
	    
		address[0] = 44 + size[0];
		for(int i=1;i<size.length;i++) {
			address[i] = address[i-1]+size[i];
		}
		for(int i=0;i<address.length;i++) {
			bb.put(intByteArrayMaker(address[i]));
		    System.out.println(bb.position());
		}
	    printBufferInfo(bb);
	    System.out.println(bb.remaining());

	    bb.put((" ").getBytes("UTF-8"));
	    System.out.println(bb.remaining());
	    
		
//	    ByteBuffer cc = ByteBuffer.allocate(5);
//	    cc.put("1".getBytes("utf-8"));
//	    byte[] dd = intByteArrayMaker(4096);
//	    bb.put(dd);
//	    System.out.println(bb.position());
//	    printBufferInfo(bb);
//	    int ee = byteArrayToInt(dd);
//	    System.out.println(ee);
	    byte[] ff = new byte[4];
	    for (int i = 0; i < 4; i++) {
	    		ff[i]=bb.get(i+4);
		    }
	    System.out.println(ff);
	    System.out.println(byteArrayToInt(ff));
	    System.out.println(bb.remaining());

	    
	}
	
	
	
	  public static void printBufferInfo(ByteBuffer bb) {
		    int limit = bb.limit();
		    System.out.println("Position =  " + bb.position() + ", Limit   = " + limit);
		    for (int i = 0; i < limit; i++) {
		      System.out.print(bb.get(i) + "  ");
		    }
		    System.out.println();
		  }
	  
	  
		public static byte[] intByteArrayMaker(int a) {
			return new byte[] {
					(byte)((a>>24) & 0xFF),
					(byte)((a>>16) & 0xFF),
					(byte)((a>>8) & 0xFF),
					(byte)(a & 0xFF)

			};
		}
		
		public static int byteArrayToInt(byte[] b) {
			return b[3]& 0xFF|
					(b[2]& 0xff)<< 8|
					(b[1]& 0xff)<< 16|
					(b[0]& 0xff)<< 24;
					
		}
}
