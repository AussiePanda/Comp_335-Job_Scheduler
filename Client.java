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
   ArrayList<String[]> ogServers = new ArrayList<String[]>();
   ArrayList<String[]> allServers = new ArrayList<String[]>();
   ArrayList<String[]> serverList = new ArrayList<String[]>();
   String sortType;
   long time = System.currentTimeMillis();
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
       ogservers();
       ogServers = sortList(ogServers);

//        for(int i = 0; i < ogServers.size(); i++){
//            System.out.println(ogServers.get(i)[0] + " " + ogServers.get(i)[1] + " " + ogServers.get(i)[4]);
//        }

       //running the first job with the server that we just found
       inputArrayString = firstJob.split(" ");
       //openServer(inputArrayString);
       assignServer();

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

   public void ogservers() {
       ogServers.clear();
       String resc = "RESC All\n";
       String[] buffer = {" "};
       ogServers.add(buffer);
       try{
           out.write(resc.getBytes());
       }
       catch(IOException i)
       {
           System.out.println(i);
       }
       read();
       while(!ogServers.get(0)[0].equals(".")) {
           ok();
           while(!readReady()) {

           }
           String god=read();
           god=god+" 0";
           ogServers.add(0, god.split(" "));


           String temp = ogServers.get(0)[0].replace("DATA","");
           ogServers.get(0)[0] = temp;

       }
       ogServers.remove(ogServers.size()-1);
       ogServers.remove(0);
      
   }

   //what servers are on the menu today. Sends that list to big array.
   public void servers() {
       allServers.clear();
       String resc = "RESC All\n";
       String[] buffer = {" "};
       allServers.add(buffer);
       try{
           out.write(resc.getBytes());
       }
       catch(IOException i)
       {
           System.out.println(i);
       }

       read();

       while(!allServers.get(0)[0].equals(".")) {
           ok();
           while(!readReady()) {
           }
           allServers.add(0, read().split(" "));
           String temp = allServers.get(0)[0].replace("DATA","");
           allServers.get(0)[0] = temp;
       }
       allServers.remove(allServers.size()-1);
       allServers.remove(0);
   }

   public void openServer(String[] input) {
		/*
		 * serverList.clear(); String resc =
		 * "RESC Avail "+input[4]+" "+input[5]+" "+input[6]+ "\n"; String[] buffer =
		 * {" "}; serverList.add(buffer);
		 * 
		 * try{ out.write(resc.getBytes()); } catch(IOException i) {
		 * System.out.println(i); }
		 * 
		 * while(!readReady()) {}; read();
		 * 
		 * while(!serverList.get(0)[0].equals(".")) { ok(); while(!readReady()) {};
		 * serverList.add(0,read().split(" ")); String temp =
		 * serverList.get(0)[0].replace("DATA",""); serverList.get(0)[0] = temp;
		 * //System.out.println(serverList.get(0)[0]); }
		 * serverList.remove(serverList.size()-1); serverList.remove(0);
		 */
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

   int modCount = 0;
   public void fastFit(){
       String lastServerType = ogServers.get(ogServers.size()-1)[0];
       int lastServerCount = 0;
       for(int i = ogServers.size()-1; i > 0; i--) {
           if (ogServers.get(i)[0].equals(lastServerType)) {
               lastServerCount++;
           }
       }

       for(int j = ogServers.size()-lastServerCount; j < ogServers.size(); j++){
           if(Integer.parseInt(ogServers.get(j)[1]) == modCount){
               System.out.println(modCount);
               modCount++;
               BFFServer = ogServers.get(j);
               return;
           }

           if(modCount == lastServerCount){
               modCount = 0;
           }
       }
       return;
       //BFFServer = ogServers.get(ogServers.size()-1);
   }

   /*int startPoint = 0;
   public void fastTraverseBestFit(ArrayList<String[]> serverList){
       String[] min = ogServers.get(ogServers.size()-1);
       String[] job = inputArrayString;
       if(startPoint == 0){
           for(int i = 0; i < serverList.size(); i++) {
               String[] ser = serverList.get(i);
               if (Integer.parseInt(ser[4]) <= Integer.parseInt(min[4])) {
                   min = ser;
                   startPoint = i;
               }
           }
       } else {
           if(startPoint + 1 != serverList.size()){
               if(Integer.parseInt(job[4]) >= Integer.parseInt(serverList.get(startPoint)[4])){
                   for(int i = startPoint; i < serverList.size(); i++){
                       String[] ser = serverList.get(i+1);
                       if(Integer.parseInt(ser[2]) != 3 && Integer.parseInt(ser[2]) != 4){
                           if(Integer.parseInt(ser[4]) <= Integer.parseInt(min[4])) {
                               min = ser;
                               startPoint = i;
                           }
                       }
                   }
               } else {
                   if(Integer.parseInt(job[4]) <= Integer.parseInt(serverList.get(startPoint)[4])){
                       for(int i = startPoint; i < serverList.size(); i++){
                           String[] ser = serverList.get(i+1);
                           if(Integer.parseInt(ser[4]) <= Integer.parseInt(min[4])){
                               min = ser;
                               startPoint = i;
                           }
                       }
                   }
               }
           }
       }
       BFFServer = min;
   }*/


   public void retard() {
       int hold =ogServers.size()-1;
       
       String[] min = ogServers.get(ogServers.size()-1);
       for(int i = 0;i<=ogServers.size();i++) {
           String[] ser = ogServers.get(i);
           if (Integer.parseInt(ser[4]) >= Integer.parseInt(inputArrayString[4])) {
               if(Integer.parseInt(ser[4]) <=Integer.parseInt(min[4])) {
                   min=ser;
                   hold=i;
                   i++;
                   while(i<=ogServers.size()-1) {
                	   ser = ogServers.get(i);
						/*
						 * System.out.println(min[0].replaceAll("\\s","").equals(ser[0].replaceAll("\\s"
						 * ,""))); System.out.println(min[0]); System.out.println(ser[0]);
						 */
                       if(min[0].replaceAll("\\s","").equals(ser[0].replaceAll("\\s",""))) {
                           if (Integer.parseInt(ser[7]) < Integer.parseInt(min[7])) {
                        	  
                        	   min = ser;
                               hold = i;
                               
                           }
                       }
                       i++;
                   }
               }
           }
       }
       
       
       ogServers.get(hold)[7]=Integer.toString(Integer.parseInt(ogServers.get(hold)[7])+1);
       
       BFFServer = min;
   }

   public ArrayList<String[]> sortList(ArrayList<String[]> listToSort) {
       ArrayList < String[] > tempServerList = new ArrayList < String[] > ();
       tempServerList.add(listToSort.get(0));

       for( int i = 1; i < listToSort.size(); i++){
           for( int j = 0; j < tempServerList.size(); j++) {

               if (Integer.parseInt(tempServerList.get(j)[4]) > Integer.parseInt(listToSort.get(i)[4])) {
                   tempServerList.add(j,listToSort.get(i));
                   break;
               }
               if (Integer.parseInt(tempServerList.get(j)[4]) == Integer.parseInt(listToSort.get(i)[4])) {

                   if (Integer.parseInt(tempServerList.get(j)[5]) > Integer.parseInt(listToSort.get(i)[5])) {
                       tempServerList.add(j, listToSort.get(i));
                       break;
                   }
                   if (Integer.parseInt(tempServerList.get(j)[6]) >= Integer.parseInt(listToSort.get(i)[6])) {
                       tempServerList.add(j, listToSort.get(i));
                       break;
                   }
               }
               if(j+1 == tempServerList.size() ){
                   tempServerList.add(listToSort.get(i));
               }
           }
       }
       return tempServerList;
   }

   public void assignServer() {
		/*
		 * ArrayList<String[]> passedList = serverList; if(serverList.size() == 0){
		 * servers(); passedList = allServers; }
		 * 
		 * passedList = sortList(passedList);
		 */

       if (sortType.equals("cf")) {
           retard();
       }
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
               System.out.println(System.currentTimeMillis() - time);
               String quit = "QUIT\n";
               line="Over";
               out.write(quit.getBytes());
               return;
           }

           if(job.equals("JOBN")) {

               if (sortType.equals("fastfit")) {
                   fastFit();
                   schd = "SCHD " + inputArrayString[2] + " " + BFFServer[0] + " " + BFFServer[1] + "\n";
                   System.out.println("beef");
               } else {
                   openServer(inputArrayString);
                   schd = "SCHD " + inputArrayString[2] + " " + BFFServer[0] + " " + BFFServer[1] + "\n";
               }
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
		/*
		 * if(args.length==2) { if(args[0].equals("-a")) { if(args[1].equals("cf") ||
		 * args[1].equals("fastfit") || args[1].equals("ftbf")) {
		 * System.out.println(args[0]);
		 */
                  // Client client = new Client("127.0.0.1", 8096,args[1]);
	   Client client = new Client("127.0.0.1", 8096,"cf");
/*               }
           }
       }else {
           System.out.println("Please use -a (cf, ftbf or fastfit)");
           return;
       }*/
   }
}
