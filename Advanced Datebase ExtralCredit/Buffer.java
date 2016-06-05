
public class Buffer {
	final int SIZE = 5;
	int l = SIZE;
	String [] memoryBuffer;
	
	public Buffer(){};
	
	public int loadNum(String [] data){
		
		if (data.length > SIZE) return SIZE;
		else return data.length;
	}
	
	public void loadToMem(String [] data){
		if (data.length < SIZE) {
			l = data.length;
			
		}
		//System.out.println("load size:" + l);
		memoryBuffer = new String[l];
		for (int i = 0; i < l; i++) {
			memoryBuffer[i] = data[i];
		}
	}
	public String [] getMemBuff(){
		return memoryBuffer;
	}
	public int getSize(){
		return SIZE;
	}
}
