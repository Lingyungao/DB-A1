package tool;

public class binaryMaker {
	public static void main(String[] args) 
	{
		String[] test2 = {"5","70","100","200","5901","12321","90201","90202"};
		int[] test = {5,70,100,200,5901,12321,90201,90202};
		for(int i = 0; i<test2.length; i++) {
			char[] strChar = test2[i].toCharArray();
		
			String result = "";
			for(int j =0;j<strChar.length;j++) {
				String temp = "000" + Integer.toBinaryString(Character.getNumericValue(strChar[j]))+" ";
				if(temp.length()>=5) {
					temp = temp.substring(temp.length()-5,temp.length());
				}
				result += temp;
			}		
			System.out.println(test2[i]+" will be trans to " + result);
		}
	}
}
