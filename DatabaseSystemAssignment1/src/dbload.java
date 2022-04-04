import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.SimpleDateFormat;
import java.text.ParseException;

public class dbload {
	MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();
	
	long totalMemorySize = memoryUsage.getInit();
	long usedMemorySize = memoryUsage.getUsed();
	
	
	
	public static void main(String[] args) throws IOException, ParseException,FileNotFoundException {
		dbload dbLoad;
		int pageSize = 0;
		int dataSize = 0;
		String filePath = null;
		int pageCount = 0;
		File dataFile;
	
		if(args.length!= 3 || !args[0].equals("-p") ||  !isNum(args[1]) ) {
			System.out.println("Arguments wrong");
			return;
		}
		
		filePath = args[2];
		dataFile = new File(filePath);
		if(!dataFile.exists()) {
			System.out.println("Can't find file");
			return;
		}
		
		pageSize = Integer.parseInt(args[0]);
		filePath = args[1];
		pageCount = 1;
		dataSize = pageSize-4;
		
		fileLoad(pageSize,filePath,pageCount,filePath);
		
	}

	private static void fileLoad(int pageSize, String dataFile, int pageCount,String filePath) {
		int counter = 0;
		int pageCounter = 0;
		int totalCounter = 0;
		int totalSize = 0;
		BufferedReader br = null;
		FileReader fr = null;
		try {
			fr = new FileReader(filePath);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		DataOutputStream dos = null;
		String ln;
		String[] rowdata;
		ArrayList<String> pageDatas = new ArrayList<String>();
		ByteBuffer bb = ByteBuffer.allocate(4096);
		try {
			br = new BufferedReader(fr);
			dos = new DataOutputStream(new FileOutputStream("./heap." + pageSize));
			// skip title
			ln = br.readLine();
			while ((ln = br.readLine()) != null){
				rowdata = ln.split(",");
				
				int id = totalCounter;
				int size[] = new int[10];
				int address[] = new int[10];
				
				String personName = rowdata[0];
				byte[] pn = personName.getBytes("utf-8");
				size[0] = pn.length;
				String birthDate = rowdata[1];
				byte[] bd = birthDate.getBytes("utf-8");
				size[1] = bd.length;
				
				String birthPlaceLabel = rowdata[2];
				byte[] bpl = birthPlaceLabel.getBytes("utf-8");
				size[2] = bpl.length;
				
				String deathDate = rowdata[3];
				byte[] dd = deathDate.getBytes("utf-8");				
				size[3] = dd.length;
				
				String fieldLabel = rowdata[4];
				byte[] fl = fieldLabel.getBytes("utf-8");
				size[4] = fl.length;
				
				String genreLabel = rowdata[5];
				byte[] gl = genreLabel.getBytes("utf-8");
				size[5] = gl.length;
				
				String instrumentLabel = rowdata[6];
				byte[] il = instrumentLabel.getBytes("utf-8");
				size[6] = il.length;
				
				String nationalityLabel = rowdata[7];
				byte[] nl = nationalityLabel.getBytes("utf-8");
				size[7] = nl.length;
				
				String thumbnail = rowdata[8];
				byte[] thu = thumbnail.getBytes("utf-8");
				size[8] = thu.length;
				
				String wikiPageID = rowdata[9];
				byte[] wpID = wikiPageID.getBytes("utf-8");
				size[9] = wpID.length;
				
				String description = rowdata[10];
				byte[] des = description.getBytes("utf-8");
				size[10] = des.length;				
				
				//init address start point include dataID and address record
				address[0] = 44 + size[0];
				//calculate address
				for(int i=1;i<size.length;i++) {
					address[i] = address[i-1]+size[i];
				}
				
				//total Size for one record = final position of record and 1 free space
				totalSize = address[10] + 1;
				
				//check space
				if((int) bb.remaining()> totalSize) {
				//fill in address and id to head of one record
				bb.put(intByteArrayMaker(id));
				for(int i=0;i<size.length;i++) {
					bb.put(intByteArrayMaker(address[i]));
				}
				//fill in record
				bb.put(pn);
				bb.put(bd);
				bb.put(bpl);
				bb.put(dd);
				bb.put(fl);
				bb.put(gl);
				bb.put(il);
				bb.put(nl);
				bb.put(thu);
				bb.put(wpID);
				bb.put(des);
				//free space
				bb.put(("#").getBytes("UTF-8"));
				}
				else {
					dos.write(bb.array());
					
					bb.clear();
				}
			}
		} catch (IOException e) {
            e.printStackTrace();
		}
		
	}
	
	public void pageMaker(byte[] page, byte[] row,int PAGE_OFFSET) {
        System.arraycopy(row,0,page,PAGE_OFFSET,row.length);
	}
	
	public static boolean isNum(String str) {
		Pattern pattern = Pattern.compile("\\d+");
		Matcher isNum = pattern.matcher(str);
		if(!isNum.matches()) {
			return false;
		}
		return true;
	}
	public static String intBinaryMaker(String str) {
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
