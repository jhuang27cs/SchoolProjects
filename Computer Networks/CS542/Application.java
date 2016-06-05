import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
public class Application {
	public static void main(String [] args){
		
		String fileName;
		String li,resp;
		int [] ar,ar2,ar3,ar4,ar5,ar6;
		int rotNum=0, i=0,j=0, source,dest,source1,dest1,fRouter;
		ArrayList<int []> tableArray=new ArrayList<int []>();
		Router soc,det,soc1,det1,downRouter;
		try{
			//input file
			Scanner sc1=new Scanner(System.in);
			System.out.println("Welcome! ");
			System.out.println("Enter the input file name: ");
			fileName=sc1.nextLine();
			
			//upload the file into matrix
			Scanner sc2=new Scanner(new BufferedReader(new FileReader(fileName)));
			while(sc2.hasNextLine()){
				sc2.nextLine();
				rotNum++;
			}
			int[][] matx= new int[rotNum][rotNum];
			Scanner sc3=new Scanner(new BufferedReader(new FileReader(fileName)));
			while(sc3.hasNext()){
					li=sc3.next();
					matx[i][j]=Integer.parseInt(li);
					if(j<rotNum-1){
					   j++;
					}
					else{
						j=0;
					    i++;
					}
				}
			
			//output the matrix
			System.out.println("----------------------------------");
			System.out.println("The initial matrix is as follows:");
			for(int m=0;m<rotNum;m++){
				System.out.println();
				for(int n=0;n<rotNum;n++){
				System.out.print(matx[m][n]+"\t ");
				}
			 }
			
			//initial the routers
			
			for(int m=0;m<rotNum;m++){
				ar=new int [rotNum];
				for(int n=0;n<rotNum;n++){
					ar[n]=matx[m][n];
					}
				tableArray.add(ar);
			}
			//Create a content of routers
			RouterTable rTable=new RouterTable();
			for(int m=0;m<rotNum;m++){
				Router re=new Router(m,tableArray.get(m));//initialize each router
				rTable.add(re);
			}
			for(int m=0;m<rotNum;m++){
				rTable.pop(m).findNeigb(rTable);//find neighbor of each router
			}
			for(int m=0;m<rotNum;m++){
				rTable.pop(m).send(rTable);//start update the table of each router
			}
			
			//output the number of iterations
			System.out.println();
			System.out.println("----------------------------------");
			System.out.print("The required number of iterations n = ");
			System.out.print(rTable.pop(0).getIte());
			
			//output the final matrix
			System.out.println();
			System.out.println("----------------------------------");
			System.out.println("The final matrix computed by the DV algorithm is as follows: ");
			for(int m=0;m<rotNum;m++){
				for(int k=0;k<rTable.pop(m).getTable().size();k++){
					if(rTable.pop(m).getTable().get(k)<999){
						System.out.print(rTable.pop(m).getTable().get(k)+"\t ");
					}else
					{
						System.out.print("NA"+"\t ");
					}
				}
				System.out.println();
			}
			
			//output the shortest path repeatedly
			do{
				//make the user input the source and destination
				System.out.println();
				System.out.println("----------------------------------");
				System.out.println("Enter the source and destination nodes: ");
				Scanner sc4=new Scanner(System.in);
				source=sc4.nextInt();
				dest=sc4.nextInt();
				soc=rTable.pop(source-1);
				det=rTable.pop(dest-1);
				FindPath fp=new FindPath();
				fp.sPath(soc, det);
				
		        //output the length of path from source to destination
				System.out.println("----------------------------------");
				System.out.print("The length of this path is :");
				System.out.println("\t"+fp.pLength(soc,det));
				
				//provide an other chance to find the path
				System.out.println("Another source-destination pair ?(yes/no)");
				Scanner sc5=new Scanner (System.in);
				resp=sc5.next();
			}while(resp.equals("yes"));
			
			//make the user enter the router which they want to set down
			System.out.println("----------------------------------");
			System.out.println("Enter the number of the router whose failure is to be simulated:");
			Scanner sc6=new Scanner(System.in);
			fRouter=sc6.nextInt();
			downRouter=rTable.pop(fRouter-1);
			downRouter.setDown(rTable);
			
			//output the number of iterations
			System.out.println();
			System.out.println("----------------------------------");
			System.out.print("The required number of iterations n = ");
			System.out.print(rTable.pop(0).getIte());
			
			//output the final matrix
			System.out.println();
			System.out.println("----------------------------------");
			System.out.println("The final matrix computed by the DV algorithm is as follows: ");
			for(int m=0;m<rotNum;m++){
				for(int k=0;k<rTable.pop(m).getTable().size();k++){
					if(rTable.pop(m).getTable().get(k)<999){
					System.out.print(rTable.pop(m).getTable().get(k)+" \t");
					}else
					{
					System.out.print("NA"+" \t");//set the distance of unreachable router to NA
					}
				}
				System.out.println();
			}
			
			//output the shortest path repeatedly
			do{
				System.out.println();
				System.out.println("----------------------------------");
				System.out.println("Enter the source and destination nodes: ");
				Scanner sc4=new Scanner(System.in);
				source1=sc4.nextInt();
				dest1=sc4.nextInt();
				soc1=rTable.pop(source1-1);
				det1=rTable.pop(dest1-1);
				FindPath fp1=new FindPath();
				fp1.sPath(soc1, det1);
		
				//output the length of path from source to destination
				System.out.println("----------------------------------");
				System.out.print("The length of this path is :");
				System.out.println("\t"+fp1.pLength(soc1, det1));
		
				//provide an other chance to find the path
				System.out.println("Another source-destination pair ?(yes/no)");
				Scanner sc5=new Scanner (System.in);
				resp=sc5.next();
			}while(resp.equals("yes"));
			System.out.println();
			System.out.println("----------------------------------");
			System.out.println("Thanks for using");
		
			
		}catch(IOException io){
			System.out.println(io);
		}
		}
}

