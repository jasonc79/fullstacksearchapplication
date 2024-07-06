package fsd.week1.layoutgrid;

/*
 * RecordsDatabaseServer.java
 *
 * The server main class.
 * This server provides a service to access the Records database.
 *
 * author: 2744544
 *
 */
import java.net.Socket;

import java.net.ServerSocket;
import java.net.InetAddress;



public class RecordsDatabaseServer {

    private int thePort = 0;
    private String theIPAddress = null;
    private ServerSocket serverSocket =  null;
	
	//Support for closing the server
	//private boolean keypressedFlag = false;
	

    //Class constructor
    public RecordsDatabaseServer(){
        //Initialize the TCP socket
        thePort = fsd.week1.layoutgrid.Credentials.PORT;
        theIPAddress = fsd.week1.layoutgrid.Credentials.HOST;

        //Initialize the socket and runs the service loop
        System.out.println("Server: Initializing server socket at " + theIPAddress + " with listening port " + thePort);
        System.out.println("Server: Exit server application by pressing Ctrl+C (Windows or Linux) or Opt-Cmd-Shift-Esc (Mac OSX)." );
        try {
            //Initialize the socket
            int maxConnectionQueue = 3;
            serverSocket = new ServerSocket(thePort,maxConnectionQueue, InetAddress.getByName(theIPAddress));
            System.out.println("Server: Server at " + theIPAddress + " is listening on port : " + thePort);
        } catch (Exception e){
            //The creation of the server socket can cause several exceptions;
            //See https://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html
            System.out.println(e);
            System.exit(1);
        }
    }

    //Runs the service loop
    public void executeServiceLoop()
    {
        System.out.println("Server: Start service loop.");
        try {
            //Service loop
            while (true) {
                // Listens for any client connections
				Socket clientSocket = serverSocket.accept();
                // Handle client connection in a separate thread
                RecordsDatabaseService tmpServiceThread = new RecordsDatabaseService(clientSocket);
            }
        } catch (Exception e){
            //The creation of the server socket can cause several exceptions;
            //See https://docs.oracle.com/javase/7/docs/api/java/net/ServerSocket.html
            System.out.println("Hello " + e);
        }
        System.out.println("Server: Finished service loop.");
        System.exit(0);
    }


/*
	@Override
	protected void finalize() {
		//If this server has to be killed by the launcher with destroyForcibly
		//make sure we also kill the service threads.
		System.exit(0);
	}
*/
	
    public static void main(String[] args){
        //Run the server
        RecordsDatabaseServer server=new RecordsDatabaseServer(); //inc. Initializing the socket
        server.executeServiceLoop();
        System.out.println("Server: Finished.");
        System.exit(0);
    }


}
