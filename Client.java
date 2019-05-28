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

        for(int i = 0; i < ogServers.size(); i++){
            System.out.println(ogServers.get(i)[0] + " " + ogServers.get(i)[1] + " " + ogServers.get(i)[4]);
        }

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

            ogServers.add(0, read().split(" "));
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
        //allServers.remove(allServers.size()-1);
        allServers.remove(0);
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
            String temp = serverList.get(0)[0].replace("DATA","");
            serverList.get(0)[0] = temp;
            //System.out.println(serverList.get(0)[0]);
        }
        serverList.remove(serverList.size()-1);
        //serverList.remove(serverList.size()-1);
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

    public void firstFit(ArrayList<String[]> serverList) {
        BFFServer = serverList.get(serverList.size()-1);
    }


    public void bestFit(ArrayList<String[]> serverList) {
        String[] min = ogServers.get(ogServers.size()-1);
        for(int i = 0;i<=serverList.size()-1;i++) {
            String[] ser = serverList.get(i);
            String[] job = inputArrayString;
            for(int j = 0; j < ogServers.size(); j++) {
                String[] og = ogServers.get(j);
                if (!ser[0].equals(".") || !ser[0].equals(" ")) {
                    if (og[0].equals(ser[0]) && og[1].equals(ser[1])) {
                        if (Integer.parseInt(job[4]) <= Integer.parseInt(og[4])) {
                            if (Integer.parseInt(ser[4]) <= Integer.parseInt(min[4])) {
                                BFFServer = ser;
                                return;
                            }
                        }
                    }
                }
            }
        }
        BFFServer = min;
    }

/*    public void bestFit(ArrayList<String[]> sList) {
        String[] min = ogServers.get(0);
        for(int i =0;i<=sList.size()-1;i++) {
            String[] ser = sList.get(i);
            String[] job = inputArrayString;
            //System.out.println(ser[0] + " " + ser[1] + " " + ser[2] + " " +ser[3] + " " + ser[4] + " " + ser[5] +" " + job[2]);
            for(int j = 0; j < ogServers.size(); j++){
                String[] og = ogServers.get(j);
                //System.out.println("Min info: " + min[0] + " " + min[1] + " " + min[4] + " Job Cores: " + job[4] + " OG Cores: " + og[4]);
                //System.out.println(serverList.get(i)[0]);
                if(!ser[0].equals(".") || !ser[0].equals(" ")) {
                    if(og[0].equals(ser[0]) && og[1].equals(ser[1])) {
                        if(Integer.parseInt(ser[2]) != 3 && Integer.parseInt(ser[2]) != 4){
                            if(Integer.parseInt(job[4]) >= Integer.parseInt(og[4])){
                                if (Integer.parseInt(ser[4]) <= Integer.parseInt(min[4]) && Integer.parseInt(ser[4]) != 0){
                                    min = ser;
                                }
                            }
                        }
                    }
                }
                BFFServer = min;
            }
        }
    }*/


    public void worstFit(ArrayList<String[]> serverList) {
        String[] max = serverList.get(0);
        for(int i =1;i<=serverList.size()-1;i++) {
            String[] ser = serverList.get(i);
            //System.out.println(serverList.get(i)[0]);
            if(!ser[0].equals(".") || !ser[0].equals(" ")) {
                if(Integer.parseInt(ser[4])>= Integer.parseInt(max[4])) {
                    max = ser;
                }
            }
        }
        BFFServer = max;
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
        ArrayList<String[]> passedList = serverList;
        if(serverList.size() == 0){
            servers();
            passedList = allServers;
        }

        passedList = sortList(passedList);

        if (sortType.equals("bf")) {
            bestFit(passedList);
        }else if (sortType.equals("wf")) {
            worstFit(passedList);
        }else if (sortType.equals("ff")) {
            firstFit(passedList);
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
        if(args.length==2)  {
            if(args[0].equals("-a")) {
                if(args[1].equals("ff") || args[1].equals("wf") ||args[1].equals("bf") ) {
                    System.out.println(args[0]);
                    Client client = new Client("127.0.0.1", 8096,args[1]);
                }
            }
        }else {
            System.out.println("Please use -a (ff, wf, bf)");
            return;
        }
    }
}
