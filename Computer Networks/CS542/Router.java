import java.util.ArrayList;

public class Router {
	ArrayList <Integer> table=new ArrayList<Integer>();
	ArrayList <Integer> copTb;
	ArrayList <Router> neigb=new ArrayList<Router>();
	
	Router [] nextHop;
	public static int iter=0;
	
	int routName;
	boolean flag;
	
	//constructor of Router class
	public Router(int rtn, int[] ary){
		routName=rtn;
		nextHop=new Router[ary.length];
		for(int i=0;i<ary.length;i++)
		{
			table.add(ary[i]);//add the initial record of router distance
		}
		
	};
	
	//find the neighbor of router
	public void findNeigb(RouterTable rt){
		for(int i=0;i<table.size();i++)
			{
				//Check the initial distance to find the neighbor
				if(table.get(i)!= 999 && table.get(i)!=0)
				{
					neigb.add(rt.pop(i));//record the neighbor router 
					nextHop[i]=rt.pop(i);//update the nexthop 
				}
			}
		
	}
	
	//send the record to the neighbor router
	public void send(RouterTable rt){
		for(int i=0;i<neigb.size();i++){
			neigb.get(i).updt(routName,table,rt);
		}
	}
	
	//update the routing table by using the received record 
	public void updt(int rName, ArrayList<Integer> tb, RouterTable rt){
		copTb=new ArrayList<Integer>();//copy the received record
		flag=false;
		for(int i=0;i<tb.size();i++)
		{
			copTb.add(tb.get(i));
		}
		for (int i=0;i<tb.size();i++)
		{
			copTb.set(i,copTb.get(i)+table.get(rName));
			//check if update is needed
			if(copTb.get(i)<table.get(i)){
				table.set(i,copTb.get(i));
				nextHop[i]=rt.pop(rName);
				flag=true;//if the record is updated, set flag to true
			}else if(copTb.get(i)>table.get(i) && nextHop[i]==rt.pop(rName)){
				table.set(i, copTb.get(i));
				flag=true;//if the record is updated, set flag to true
			}
			
		}
		//if the table is updated, then call the send method
		if(flag==true){
			iter++;
			this.send(rt);
		}
	}

	//return the record distance of the router
	public ArrayList<Integer> getTable()
	{
		for(int i=0;i<table.size();i++)
		{
			if(table.get(i)>999)
			{
				table.set(i,999);
			}
		}
		return table;
	}
	
	//return the iteration times
	public int getIte(){
		return iter;
	}
	
	//return the nextHop of destination router 
	public Router getNextHop(int i){
		return nextHop[i];
	}
	
	//set this router down
	public void setDown(RouterTable rt){
		iter=0;
		for(int i=0;i<table.size();i++)
		{
			table.set(i, 999);
		}
		this.send(rt);
	}
}
