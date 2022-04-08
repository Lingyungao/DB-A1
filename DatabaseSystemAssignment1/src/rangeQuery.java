import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class rangeQuery {

	public static void main(String[] args) {
		String filePath = null;
		File dataFile;
		int searchKey1;
		int searchKey2;

		if (args.length != 3 || !isNum(args[1]) || !isNum(args[2])) {
			System.out.println("Arguments wrong");
			return;
		}

		filePath = "heap.4096";
		dataFile = new File(filePath);

		if (!dataFile.exists()) {
			System.out.println("Can't find file");
			return;
		}

		if (Integer.parseInt(args[1]) > Integer.parseInt(args[2])) {
			searchKey1 = Integer.parseInt(args[2]);
			searchKey2 = Integer.parseInt(args[1]);
		} else {
			searchKey1 = Integer.parseInt(args[1]);
			searchKey2 = Integer.parseInt(args[2]);
		}

		fileSearch(searchKey1, searchKey2, filePath);
	}

//	[0-4]:totalID 
//	[4-8]:pageID
//	[8-12]:recordID;
//  [12-16]:name;
//	[16-20]:birth data;
//  [0-56]: head
	private static void fileSearch(int searchKey1, int searchKey2, String filePath) {
		long startTime = System.currentTimeMillis();
		BufferedReader br = null;
		FileReader fr = null;
		File file = new File(filePath);
		int pageCounter = 1;

		int totalID = 0;
		int pageID = 0;
		int recordID = 0;
		int searchCounter = 1;

		try {
			fr = new FileReader(filePath);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String[] rowdata;

		byte[] page = new byte[4096];
		int tempByte;
		FileInputStream fileInputStream;

		try {
			int startAddress;
			int endAddress;
			int dataSize;
			int birthdayAddress;
			int birthData;
			int finalEnd = 0;
			String name = "";
			fileInputStream = new FileInputStream(file);
			BufferedInputStream bis = new BufferedInputStream(fileInputStream);
			br = new BufferedReader(fr);

			while ((tempByte = bis.read(page)) != -1) {
				byte[] ff = new byte[4];
				int hasnext = 1;
				int nextRecordAddress = 0;

				while (hasnext != 0) {
					startAddress = makeFourBytesToInt(page, 16+nextRecordAddress);
					endAddress = makeFourBytesToInt(page, 20+nextRecordAddress);
					// sometime will null
					dataSize = endAddress - startAddress;
					totalID = makeFourBytesToInt(page,nextRecordAddress);
					pageID = makeFourBytesToInt(page,nextRecordAddress+4);
					recordID = makeFourBytesToInt(page,nextRecordAddress+8);
					name = getStringByStartPoint(page, 12, nextRecordAddress);
					if (dataSize != 0) {
						birthdayAddress = startAddress + nextRecordAddress;
						birthData = makeFourBytesToInt(page, birthdayAddress);
						if (birthData >= searchKey1 && birthData <= searchKey2) {
							System.out.println("record: " + totalID + " as " + recordID + " record of page: " + pageID
									+ " match the result for birthData: " + birthData + " for name " + name);
						}
						}
						else {
							birthData = 0;
						}

						finalEnd = makeFourBytesToInt(page, 56 + nextRecordAddress);
						nextRecordAddress = finalEnd + nextRecordAddress + 1;
						searchCounter++;
						if(nextRecordAddress>=4092) {
							nextRecordAddress = 0;
						}
						
						hasnext = makeFourBytesToInt(page, nextRecordAddress);
						if(hasnext!=totalID+1) {
							hasnext = 0;
						}
						if(hasnext==0) {
							nextRecordAddress = 0;
							break;
						}

					}
					pageCounter++;
				}System.out.println(bis.readAllBytes().length + " byts data readed");			
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Searched " + pageCounter + " pages");

		System.out.println("All search done, ");
		long endTime = System.currentTimeMillis();
		System.out.println("Time cost: " + (endTime - startTime) + "ms");

	}

	public static boolean isNum(String str) {
		Pattern pattern = Pattern.compile("\\d+");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	public static int byteArrayToInt(byte[] b) {
		return b[3] & 0xFF | (b[2] & 0xff) << 8 | (b[1] & 0xff) << 16 | (b[0] & 0xff) << 24;
	}

	public static void printBufferInfo(ByteBuffer bb) {
		int limit = bb.limit();
		System.out.println("Position =  " + bb.position() + ", Limit   = " + limit);
		for (int i = 0; i < 4; i++) {
			System.out.print(bb.get(i) + "  ");
		}
		System.out.println();
	}

	public static byte[] getFourBytes(ByteBuffer bb, int location) {
		byte[] tempByte = new byte[4];
		for (int i = 0; i < 4; i++) {
			tempByte[i] = bb.get(location + i);
		}
		return tempByte;
	}

	public static byte[] getFourBytes(byte[] page, int location) {
		byte[] tempByte = new byte[4];
		for (int i = 0; i < 4; i++) {
			tempByte[i] = page[location + i];
		}
		return tempByte;
	}

	public static byte[] getStringBytes(byte[] page, int location, int num) {
		byte[] tempByte = new byte[num];
		for (int i = 0; i < num; i++) {
			tempByte[i] = page[i + location];
		}
		return tempByte;
	}

	public static int makeFourBytesToInt(ByteBuffer bb, int location) {
		return byteArrayToInt(getFourBytes(bb, location));
	}

	public static int makeFourBytesToInt(byte[] page, int location) {
		return byteArrayToInt(getFourBytes(page, location));
	}

	public static String makeBytesToString(byte[] page, int location, int num) {
		String str = new String(getStringBytes(page, location, num));
		return str;
	}

	public static String getStringByStartPoint(byte[] page, int address, int start) {

		int startLocation = address + start;
		int endLocation = startLocation + 4;
		int startInData = makeFourBytesToInt(page, startLocation);
		int endInData = makeFourBytesToInt(page, endLocation);
		int size = endInData - startInData;
		if (size > 0) {
			String str = makeBytesToString(page, startInData + start, size);
			return str;
		} else
			return "";

	}

	public static void fullDataCheck(byte[] page, int startPoint) {
		System.out.println("StartPoint:" + startPoint);
		System.out.print(getStringByStartPoint(page, 12, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 20, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 28, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 32, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 36, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 40, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 44, startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(page, 52, startPoint));
		System.out.println(" || ");

	}
}
