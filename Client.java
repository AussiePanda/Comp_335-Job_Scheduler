
// A Java program for a Client 
import java.net.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.io.*; 

public class Client 
{ 
    // initialize socket and input output streams 
    private Socket socket            = null; 
    private DataInputStream  input   = null; 
    private DataOutputStream out     = null; 
    private DataInputStream veto = null;

    // Strings, arrays and arrayList to hold server responses and list of avaliable servers.
    String line = "";
    String[] inputArrayString;
    String[] bestFitServer;
    ArrayList<String[]> serverList = new ArrayList<String[]>();

    // constructor to put ip address and port 
    public Client(String address, int port)
    { 
        // establish a connection 
        try
        {
            socket = new Socket(address, port); 
            System.out.println("Connected"); 
  
            // takes input from terminal 
            input = new DataInputStream(System.in); 
  
            // sends output to the socket 
            out = new DataOutputStream(socket.getOutputStream());
            veto = new DataInputStream(socket.getInputStream());
        } 
        catch(UnknownHostException u) 
        { 
            System.out.println(u); 
        } 
        catch(IOException i) 
        { 
            System.out.println(i); 
        }

        //sends HELO to the server to start sending job information
        hello();
        // making sure the client waits for the server's reply, without getting stuck in an infinite loop
        for(int i=0;i<1000;i++) {
        	if(readReady()) {
        		i=1000;
        	}
        }
        //reads the first 2 responses and sends OK for
        read();
        //sends the first ready request
        ready();
        for(int i=0;i<1000;i++) {
        	if(readReady()) {
        		i=1000;
        	}
        }
    	//holds the name of the first task for use once we know the server to use
        String firstJob = read();
               
        //running the first job with the server that we just found
        inputArrayString = firstJob.split(" ");
        openServer(inputArrayString);
        String first = "SCHD " + inputArrayString[2]+" " + bestFitServer[0] +" "+bestFitServer[1] + "\n";

        try {
			out.write(first.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //and it begins
        ready();
        while (!line.equals("Over")) 
        { 
        	schedule();
        	//main loop	   	        	
        } 
        // close the connection 
        try
        { 
            input.close(); 
            out.close(); 
            socket.close(); 
        } 
        catch(IOException i) 
        { 
            System.out.println(i); 
        } 
        finally {
        	
        }
    }

    //functions
	//Sends HELO to the server and the AUTH plus name, has a slight delay so that client doesn't send messages before
	//server is ready
	public void hello() {
		String helo = "HELO\n";
		String auth = "AUTH xxx\n";

		try{
			out.write(helo.getBytes());
			TimeUnit.MILLISECONDS.sleep(5);
			out.write(auth.getBytes());
		}
		catch(IOException i)
		{
			System.out.println(i);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
   //our read function returns whatever there is to read, there should always be something, but we still check
    public String read() {
    	String holding = "";
		try {
		//our little check
			if(veto.available()!=0) {
				for(int i = 0;i<veto.available();) {
					holding=holding+ (char)veto.read();
				} 	
			}
      	}
      	catch(IOException i){
      		System.out.println(i);
      	}
		holding.trim();
		holding.trim();
		return holding;
	}

	//tells the server you're ready for another job
	public void ready() {
		String ready = "REDY\n";
		try{
			out.write(ready.getBytes());
		}
		catch(IOException i)
		{
			System.out.println(i);
		}
	}

    //runs the same check that is in the read function, is used as a brake for the whole program
	//so that we are not speeding along (at the speed of light) faster than the server
	public boolean readReady() {
		try {
			if(veto.available()!=0) {
				return true;	
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	//Sends OK to the server
 	public void ok () {
	 String ok = "OK\n";
	 
	try {
		out.write(ok.getBytes());
	} catch (IOException e) {
		e.printStackTrace();
	}
 }

	//will fill serverList with a list of the avaliable servers that fit the criteria
 	public void openServer(String[] input) {
 		serverList.clear();
 		String resc = "RESC Avail "+input[4]+" "+input[5]+" "+input[6]+ "\n";
 		String[] buffer = {" "};
    	serverList.add(buffer);
    	   		
    	try{    		   		
   		out.write(resc.getBytes());
    	}
    	 catch(IOException i) 
        { 
            System.out.println(i); 
        }     	
    	while(!readReady()) {};
    	read();	
    	
    	while(!serverList.get(0)[0].equals(".")) {   		
    		ok();
    		while(!readReady()) {};
    		serverList.add(0,read().split(" "));
    		serverList.get(0)[0].replace("DATA","");
    	}
    	serverList.remove(serverList.size()-1);
    	serverList.remove(serverList.size()-1);
    	serverList.remove(0);
    	 assignServer();
 	}

 	//will traverse the serverList to find the most suited server for handling the job that needs to be scheduled
 	public void assignServer() {
    	String[] min = serverList.get(0);
	    for(int i =1;i<=serverList.size()-1;i++) {
	    	String[] ser = serverList.get(i);
	    	if(!ser[0].equals(".") || !ser[0].equals(" ")) {
	    		if(Integer.parseInt(ser[4])<= Integer.parseInt(min[4])) {
	    			min = ser;
			   }
		   }
	   }
	   bestFitServer = min;
   }
   
    //After the first job is scheduled, this command is called repeatedly in a loop. The process for the first job is
	//repeated, getting the job, finding all avaliable servers that fit that job, then searching for the best fit before
	//scheduling the job. The loop terminates once the server sends NONE instead of JOBN, indicating there are no more
	//jobs to complete
    public void schedule() {  
    	//this for loop has a catch that tries to stop the main loop continuing forever in the case that the server is doing nothing
    	for(int i=0;i<1000;i++) {
        	if(readReady()) {
        		i=1000;
        	}
        	else if(i<999) {
        		line="over";
        	}
        }
    	
    	inputArrayString = read().split(" ");
    	String job = inputArrayString[0];
    	String schd = "";
    	//assigning and writing jobs
    	try{
    	if(inputArrayString[0].equals("NONE")){
        	String quit = "QUIT\n";
        	line="Over";
        	out.write(quit.getBytes());
        	return;
        }
    	if(job.equals("JOBN")) {	
    		openServer(inputArrayString);
    		
    		schd = "SCHD " + inputArrayString[2]+" " + bestFitServer[0] +" "+ bestFitServer[1] + "\n";
    	}    	    	    	
    	if(!schd.equals("")) {
    		out.write(schd.getBytes());    		
    	}
    	//otherwise ask for something else
    	else {
    		ready();
    	}   		
    	}
    	 catch(IOException i) 
        { 
            System.out.println(i); 
        }     	    	
    }

    public static void main(String args[]){
        Client client = new Client("127.0.0.1", 8096);
    } 
} 
