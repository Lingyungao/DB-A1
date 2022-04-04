package tool;

public class hexmakertest {

	
	public static void main(String[] args)
	{
		int[] test = {5,70,100,200,5901,12321,90201,90202};
		
		for(int i = 0; i<test.length; i++) {
			
			String hex = Integer.toHexString(test[i] & 0xFFFF);
			if( hex.length()<2 ){
				hex = "000" + hex;
			}
			else if( hex.length()<3 ){
				hex = "00" + hex;
			}
			else if( hex.length()<4 ){
				hex = "0" + hex;
			}
			
			System.out.println(test[i]+" will be trans to " + hex);
		}
		

	}
}
