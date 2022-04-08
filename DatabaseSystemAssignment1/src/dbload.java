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
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import tool.byteBufferFunction;

import java.text.SimpleDateFormat;
import java.text.ParseException;

public class dbload {
	MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	MemoryUsage memoryUsage = memoryMXBean.getHeapMemoryUsage();

	long totalMemorySize = memoryUsage.getInit();
	long usedMemorySize = memoryUsage.getUsed();

	public static void main(String[] args) throws IOException, ParseException, FileNotFoundException {
		dbload dbLoad;
		int pageSize = 0;
		int dataSize = 0;
		String filePath = null;
		File dataFile;

		if (args.length != 3 || !args[0].equals("-p") || !isNum(args[2])) {
			System.out.println("Arguments wrong");
			return;
		}

		filePath = args[1];
		dataFile = new File(filePath);
		if (!dataFile.exists()) {
			System.out.println("Can't find file");
			return;
		}

		pageSize = Integer.parseInt(args[2]);
		filePath = args[1];

		fileLoad(pageSize, filePath);

	}

	private static void fileLoad(int pageSize, String filePath) {
		int recordCounter = 1;
		int pageCounter = 1;
		int totalCounter = 1;
		int totalSize = 0;
		int nextRecordStartPoint = 0;
		int rollbackRecord = 0;
		long startTime = System.currentTimeMillis();
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
		try {
			br = new BufferedReader(fr);
			dos = new DataOutputStream(new FileOutputStream("./heap." + pageSize));
			ByteBuffer bb = ByteBuffer.allocate(pageSize);
			System.out.println(bb);
			byte[] bd;
			byte[] bpl;
			byte[] dd;
			byte[] fl;
			byte[] gl;
			byte[] il;
			byte[] nl;
			byte[] thu;
			byte[] wpID;
			byte[] des;
			// skip title
			ln = br.readLine();
			while ((ln = br.readLine()) != null) {
				System.out.println("Reading record " + totalCounter + "try to fill in page " + pageCounter);
				rowdata = ln.trim().split(",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);

				int size[] = new int[12];
				int address[] = new int[12];

				String personName = rowdata[1];
				byte[] pn;
				pn = personName.getBytes("utf-8");
				size[0] = pn.length;

				String birthDate = rowdata[2];
				if (birthDate != "") {
					int birthDateToInt = Double.valueOf(rowdata[2]).intValue();
					bd = intByteArrayMaker(birthDateToInt);
				} else {
					bd = birthDate.getBytes("utf-8");
				}
//				System.out.println(birthDate);

				size[1] = bd.length;

				String birthPlaceLabel = rowdata[3];
				bpl = birthPlaceLabel.getBytes("utf-8");
				size[2] = bpl.length;

				String deathData = rowdata[4];
				if (deathData != "") {
					int deathDateToInt = Double.valueOf(rowdata[4]).intValue();
					dd = intByteArrayMaker(deathDateToInt);
				} else {
					dd = deathData.getBytes("utf-8");
				}
				size[3] = dd.length;

				String fieldLabel = rowdata[5];
				fl = fieldLabel.getBytes("utf-8");
				size[4] = fl.length;

				String genreLabel = rowdata[6];
				gl = genreLabel.getBytes("utf-8");
				size[5] = gl.length;

				String instrumentLabel = rowdata[7];
				il = instrumentLabel.getBytes("utf-8");
				size[6] = il.length;

				String nationalityLabel = rowdata[8];
				nl = nationalityLabel.getBytes("utf-8");
				size[7] = nl.length;

				String thumbnail = rowdata[9];
				thu = thumbnail.getBytes("utf-8");
				size[8] = thu.length;

				String wikiPageID = rowdata[10];
				if (wikiPageID != "") {
					int wikiPageIDToInt = Double.valueOf(rowdata[10]).intValue();
					wpID = intByteArrayMaker(wikiPageIDToInt);
				} else {
					wpID = wikiPageID.getBytes("utf-8");
				}
				size[9] = wpID.length;

				String description = rowdata[11];
//				System.out.println("description writen into data as:" + description);
				des = description.getBytes("utf-8");
				size[10] = des.length;

				// for # at end
				size[11] = "#".length();
				// init address start point include dataID and address record
				address[0] = 60; // calculate address
				for (int i = 1; i < size.length; i++) {
					address[i] = address[i - 1] + size[i - 1];
					System.out.print(i + ":" + address[i] + " |");
				}
				System.out.println("");
				// total Size for one record = final position of record and 1 free space
				String printer = "";
				String printer2 = "";

				for (int i = 0; i < size.length; i++) {
					totalSize = totalSize + size[i];
					printer = printer + (i + ":" + size[i] + " |");
					printer2 = printer2 + (i + ":" + totalSize + " |");
				}
				totalSize = totalSize+60;
				System.out.println(printer);
				System.out.println(printer2);
								
				
				// check space
				if ((int) bb.limit() > totalSize) {
					// fill in address and id to head of one record
					fillInRecord(totalCounter, pageCounter, recordCounter, size, address, bb, pn, bd, bpl, dd, fl, gl,
							il, nl, thu, wpID, des, nextRecordStartPoint, totalSize);					
					System.out.println("record " + totalCounter + ", size:" + totalSize + " fill into page "
							+ pageCounter + " as " + recordCounter + " on page");
					System.out.println("remaining space on page is " + bb.remaining());
					System.out.println("");
					totalCounter++;
					recordCounter++;
					rollbackRecord = totalSize-nextRecordStartPoint;
					nextRecordStartPoint = totalSize;
					System.out.println(bb);
					
				} else {
					System.out.println("no enough space on page");
					System.out.println("Size of page is:" + bb.array().length);
					dos.write(bb.array());
					System.out.println(bb);
					bb.clear();
					System.out.println(bb);
					pageCounter++;
					recordCounter = 0;
					nextRecordStartPoint =0;
					totalSize = 0;
					for (int i = 0; i < size.length; i++) {
						totalSize = totalSize + size[i];
						printer = printer + (i + ":" + size[i] + " |");
						printer2 = printer2 + (i + ":" + totalSize + " |");
					}
					totalSize = totalSize+ 60;
					fillInRecord(totalCounter, pageCounter, recordCounter, size, address, bb, pn, bd, bpl, dd, fl, gl,
							il, nl, thu, wpID, des, nextRecordStartPoint, totalSize);
					nextRecordStartPoint = totalSize;

					totalCounter++;
				}
			}
			System.out.println("final page");
			totalCounter--;
			recordCounter--;
			System.out.println(recordCounter + " records write into file on page " + pageCounter);
			System.out.println("total writed: " + totalCounter + " records");
			System.out.println("Size of page is:" + bb.array().length);
			dos.write(bb.array());
			dos.close();
			long endTime = System.currentTimeMillis();

			System.out.println("job done");
			System.out.println("timeCost: " + (endTime - startTime));
			System.out.println(bb);
			
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static void fillInRecord(int totalCounter, int pageCounter, int recordCounter, int[] size, int[] address,
			ByteBuffer bb, byte[] pn, byte[] bd, byte[] bpl, byte[] dd, byte[] fl, byte[] gl, byte[] il, byte[] nl,
			byte[] thu, byte[] wpID, byte[] des, int nextRecordStartPoint, int totalSize) {

		// fill in record
		bb.put(intByteArrayMaker(totalCounter));
		bb.put(intByteArrayMaker(pageCounter));
		bb.put(intByteArrayMaker(recordCounter));
		for (int i = 0; i < size.length; i++) {
			bb.put(intByteArrayMaker(address[i]));
		}
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
		// free spaceb
		bb.put("#".getBytes());
		System.out.println("start point" + nextRecordStartPoint);
		System.out.println("totalSize" + totalSize);
		String str = new String(getBytes(bb, (totalSize-1), 1));
		System.out.println("# now:" + str);
		fullDataCheck(bb,nextRecordStartPoint);

	}

	public void pageMaker(byte[] page, byte[] row, int PAGE_OFFSET) {
		System.arraycopy(row, 0, page, PAGE_OFFSET, row.length);
	}

	public static boolean isNum(String str) {
		Pattern pattern = Pattern.compile("\\d+");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	public static String intBinaryMaker(String str) {
		char[] byteData = str.toCharArray();
		String result = "";
		for (int i = 0; i < byteData.length; i++) {
			// remove " " later
			String temp = "000" + Integer.toBinaryString(Character.getNumericValue(byteData[i])) + " ";
			if (temp.length() >= 5) {
				temp = temp.substring(temp.length() - 5, temp.length());
			}
			result += temp;
		}
		return result;
	}

	public static byte[] intByteArrayMaker(int a) {
		return new byte[] { (byte) ((a >> 24) & 0xFF), (byte) ((a >> 16) & 0xFF), (byte) ((a >> 8) & 0xFF),
				(byte) (a & 0xFF)

		};
	}

	public static int byteArrayToInt(byte[] b) {
		return b[3] & 0xFF | (b[2] & 0xff) << 8 | (b[1] & 0xff) << 16 | (b[0] & 0xff) << 24;
	}

	public static void printBufferInfo(ByteBuffer bb) {
		int limit = bb.limit();
		System.out.println("Position =  " + bb.position() + ", Limit   = " + limit);
		for (int i = 0; i < limit; i++) {
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

	public static byte[] getBytes(ByteBuffer bb, int location, int num) {
		byte[] tempByte = new byte[num];
		for (int i = 0; i < num; i++) {
			tempByte[i] = bb.get(location + i);
//			System.out.print(bb.get(location+i)+"|");	
		}
		return tempByte;
	}

	public static int makeFourBytesToInt(ByteBuffer bb, int location) {
		return byteArrayToInt(getFourBytes(bb, location));
	}

	public static String makeBytesToString(ByteBuffer bb, int location, int num) {
		String str = new String(getBytes(bb, location, num));
		return str;
	}

	public static String getStringByStartPoint(ByteBuffer bb,int Location, int start) {
		int startLocation = Location+start;
		int endLocation = startLocation + 4;
		int startInData = makeFourBytesToInt(bb, startLocation);
		int endInData = makeFourBytesToInt(bb, endLocation);
		int size = endInData - startInData;		
		if (size > 0) {
			String str = makeBytesToString(bb,startInData+start,size);
			return str;
		} else
			return "";
	}

	public static void fullDataCheck(ByteBuffer bb, int startPoint) {
		System.out.println("StartPoint:"+startPoint);
		System.out.print(getStringByStartPoint(bb,12,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,20,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,28,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,32,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,36,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,40,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,44,startPoint));
		System.out.print(" || ");
		System.out.print(getStringByStartPoint(bb,52,startPoint));
		System.out.println(" || ");

	}

}
