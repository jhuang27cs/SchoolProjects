
public class SortData {
	public SortData(){};
	
	public String [] sort(String [] data){
		
		for(int i = 0; i < data.length; i++){
			String smallerS  = data[i];
			for (int j = i + 1; j < data.length; j++) {
				String tempS = compare(smallerS, data[j]);
				if (!tempS.equals(smallerS)) {
					data[j] = smallerS;
					smallerS = tempS;
				}
			}
			data[i] = smallerS; 
		}
		return data;
	}
	
	public String compare(String firstS, String secondS) {
		boolean flag = true;
		int comLength = firstS.length();
		String tempF = firstS.toLowerCase();
		String tempS = secondS.toLowerCase();
		
		if(firstS.length() > secondS.length()) {
			comLength = secondS.length();
			flag = false;
		}
		for (int i =0 ; i < comLength; i++) {
			int diff = tempF.charAt(i) - tempS.charAt(i);
			
			// return the smaller one
			if (diff > 0) return secondS; 
			else if (diff < 0) return firstS;
		}
		if (flag) return firstS;
		else return secondS;
	}

}
