import java.util.ArrayList;


public class Application {
	public static void main(String [] args) {
		
		File file = new File();
		file.readFromTxt("TestCase.txt");
		ArrayList<String> dataSet = file.getFile();
		int length = dataSet.size();
		String [] data = new String[length];
		for (int i = 0; i < length; i++) {
			data[i] = dataSet.get(i);
		}
		Buffer bf = new Buffer();
		SortData sd = new SortData();
		File writeInto = new File();
		ModifyData md = new ModifyData();
		int bufferSize = bf.getSize();
		int sign = bf.loadNum(data);
		
		while (!(sign < bufferSize)) {
			bf.loadToMem(data);
			
			String [] mem = bf.getMemBuff();
			//System.out.println("Sort"+ mem[0]+mem[1]+mem[2]+mem[3]+mem[4]);
			mem = sd.sort(mem);
			
			writeInto.writeIntoFile(mem);
			data = md.modify(data, bufferSize);
			//System.out.println("data "+ data[0] + data[1]);
			sign = bf.loadNum(data);
		}
		if(sign > 0) {
			bf.loadToMem(data);
			String [] mem = bf.getMemBuff();
			mem = sd.sort(mem);
			writeInto.writeIntoFile(mem);
		}
		ArrayList<String> result = writeInto.getFile();
		for(String r : result){
			System.out.println(r);
		}
	}
}
