
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
    String line = "";
    String[] inputArrayString;
    String[] BFFServer;
    ArrayList<String[]> serverList = new ArrayList<String[]>();
    String sortType;
    // constructor to put ip address and port
    public Client(String address, int port,String Type)
    {
        // establish a connection
        try
        {
        	sortType=Type;
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

        // string to read message from input

        //hi server
        hello();
        // making sure the client waits for the server's reply, without getting stuck in an infinite loop
        for(int i=0;i<1000;i++) {
        	if(readReady()) {
        		i=1000;
        	}
        }
        //reads the first 2 responses okok
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

        //what servers do we have

        //running the first job with the server that we just found
        inputArrayString = firstJob.split(" ");
        openServer(inputArrayString);

        String first = "SCHD " + inputArrayString[2]+" " + BFFServer[0] +" "+BFFServer[1] + "\n";
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
		return holding.trim();
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

	//ok, tell the server "ok", ok, sweet.
 	public void ok () {
	 String ok = "OK\n";

	try {
		out.write(ok.getBytes());
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
 }

 	//what servers are on the menu today. Sends that list to big array.
 	public void servers() {
    	serverList.clear();
    	String resc = "RESC All\n";
    	//serverList.add("");
    	try{
    		out.write(resc.getBytes());
    	}
    	 catch(IOException i)
        {
            System.out.println(i);
        }
    	while(!serverList.get(0)[0].equals(".")) {

    		ok();
    		while(!readReady()) {

    		}
			serverList.add(0, read().split(" "));
    	}

    //	largeServer = word.get(1).split(" ");
    //	System.out.println(largeServer[0]);


    }

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
    		//System.out.println(serverList.get(0)[0]);
    	}
    	serverList.remove(serverList.size()-1);
    	serverList.remove(serverList.size()-1);
    	serverList.remove(0);
    	 assignServer();
 	}

    // asking the server for another job
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

    //greeting the server
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
 // Since the RESC avail returns all the avaliable servers and we store all of these in the order they are sent
 // we just need to pick the first one added to the arrayList to get the first fit. This method would be quicker
 //if we didn't have to go through all of the RESC avail servers and could just use the first available one
  public void firstFit() {
	  BFFServer = serverList.get(serverList.size()-1);
  }


   public void assignServer() {

		   firstFit();

   }

    //send jobs and other things
    public void schedule() {
    	//this for loop has a catch that tries to stop the main loop continuing forever in the case that the server is doing nothing
    	//our own little timeout and brakeBFFServer
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

    		schd = "SCHD " + inputArrayString[2]+" " + BFFServer[0] +" "+ BFFServer[1] + "\n";
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
    public static void main(String args[])
    {
        Client client = new Client("127.0.0.1", 8096,args[1]);

    }
}
