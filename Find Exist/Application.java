import java.util.*;
public class Application {
	public static void main(String [] args) {
		boolean flag = false;
		do {
		System.out.println("Please enter a sentence and press enter to confirm:");
		Scanner scan1 = new Scanner(System.in);
		String line = scan1.nextLine();
		System.out.println("Please enter a word and press enter to confirm");
		Scanner scan2 = new Scanner(System.in);
		String word = scan2.next();
		FindExist f = new FindExist(line, word);
		boolean result = f.contains();
		if (result) {
			System.out.println("The word is Found in the sentence!");
		} else {
			System.out.println("The word is Not Found in the sentence!");
		}
		Scanner scan3 = new Scanner(System.in);
		System.out.println ("YOU CAN ENTER 'GO' TO CONTINUE OR 'END' TO STOP:");
		String k = scan3.next();
		String key = k.toLowerCase();
		if (key.equals("go")) {
			flag = true;
		} else if (key.equals("end")){
			flag = false;
			System.out.println("Thanks for using!");
		} 
		}while (flag);

	}

}
