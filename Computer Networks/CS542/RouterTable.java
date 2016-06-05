import java.util.ArrayList;


public class RouterTable {
	ArrayList <Router> router=new ArrayList<Router>();
	
	//the constructor of RouterTable
	public RouterTable(){
		
	}
	
	//add a router into the content
	public void add(Router rot)
	{
		router.add(rot);
	}
	
	//return the i-th router
	public  Router pop(int i)
	{
		return router.get(i);
	}
	
}
