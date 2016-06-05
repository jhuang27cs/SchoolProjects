
public class ModifyData {
	public ModifyData(){}
	public String[] modify(String[] data, int offset) {
		
		if (offset > data.length) {
			return data;
		} else {
			String [] temp = new String[data.length-offset];
			for(int i = offset; i < data.length; i++) {
				temp[i-offset] = data[i];
			}
			return temp;
		}
	}
}
