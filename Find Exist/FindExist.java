import java.util.ArrayList;


public class FindExist {
	private String sentence;
	private String newWord;
	public FindExist(String sent, String word){
		sentence = sent.toLowerCase();
		newWord = word.toLowerCase();
	}
	
//	public String [] toSentArray(String sent) {
//		
//		int length = sent.length();
//		String newSent = sent.toLowerCase();
//		String newList [] = new String[length];
//		int newIndex = 0;
//		int begin = 0;
//		int end = 0;
//		for (int i = 0; i < length; i++) {
//			if (newSent.charAt(i) == ' ') {
//				end  = i;
//				newList[newIndex] = newSent.substring(begin, end);
//				begin = i+1;
//				newIndex++;
//			}
//			newList[newIndex] = newSent.substring(begin, length);
//		}
//		if (begin == 0 && end == 0) {
//			newList[0] = sent;
//		}
//		return newList;
//	}
//	
//	public boolean contains(String [] sent, String word) {
//		String newWord = word.toLowerCase();
//		for (int i = 0; i < sent.length; i++) {
//			if (sent[i] == null) {
//				return false;
//			}
//			if (sent[i].equals(newWord)) {
//				return true;
//			}
//		}
//		return false;
//	}
	public boolean contains() {
		for (int i = 0 ; i < sentence.length(); i++) {
			
			if (sentence.charAt(i) == newWord.charAt(0)) {
				if (i + newWord.length() > sentence.length()){
					return false;
				}
				String testWord = sentence.substring(i, i+newWord.length());
				if (testWord.equals(newWord)) {
					return true;
				} else {
					continue;
				}
			}
		}
		return false;
	}
}
