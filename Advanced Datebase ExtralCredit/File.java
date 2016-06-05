import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;


public class File {
	public File(){};
	ArrayList<String> file = new ArrayList<String>();
	
	public void readFromTxt(String fileName){
		try {
			FileReader fileReader = new FileReader(fileName);
			BufferedReader bufferReader = new BufferedReader(fileReader);
			String line = "";
			while((line = bufferReader.readLine()) != null) {
				file.add(line);
			}
			bufferReader.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	public void writeIntoFile(String [] array) {
		if (file.isEmpty()) {
			for (int i = 0; i < array.length; i++) {
				if (!file.contains(array[i])){
					file.add(array[i]);
				}
			}
		} else {
			ArrayList<String> temp = new ArrayList<String>();
			SortData sd = new SortData();
			int i = 0, j = 0;
			while(i < array.length || j < file.size()) {
				if (i < array.length && j < file.size()) {
					String smaller = sd.compare(array[i], file.get(j));
					if(!temp.contains(smaller)){
						temp.add(smaller);
					}
					if (smaller.equals(array[i])) i++;
					else j++;
				} else if (i < array.length) {
					if(!temp.contains(array[i])){
						temp.add(array[i]);
					}
					i++;
				} else {
					if(!temp.contains(file.get(j))){
						temp.add(file.get(j));
					}
					j++;
				}
			}
			file = temp;
		}
	}
	
	public ArrayList<String> getFile() {
		return file;
	}
}
