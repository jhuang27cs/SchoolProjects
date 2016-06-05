import java.util.ArrayList;


public class FindPath {
	Router sorc, dest;
	RouterTable rTable;
	int rNum1, rNum2;
	int count=0;
	ArrayList<Integer> path=new ArrayList<Integer>();
	
	//the constructor of FindPath
	public FindPath(){
	}
	
	//print the path from the source to the destination 
	public void sPath(Router source, Router destination){
		count++;
		rNum1=source.routName;
		rNum2=destination.routName;
		System.out.print(rNum1+1+"->");
		if(source.getNextHop(rNum2).equals(destination)){
			System.out.println(rNum2+1);
		}
		else{
			sPath(source.getNextHop(rNum2),destination);
		}
		
	}
	
	//return the path length between source and destination
	public int pLength(Router source, Router destination){
		return source.table.get(destination.routName);
	}
}
