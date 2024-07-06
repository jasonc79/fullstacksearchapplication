package fsd.week1.layoutgrid;

import java.io.BufferedReader;
//import java.io.OutputStreamWriter;
import java.io.IOException;

/*
 * RecordsDatabaseService.java
 *
 * The service threads for the records database server.
 * This class implements the database access service, i.e. opens a JDBC connection
 * to the database, makes and retrieves the query, and sends back the result.
 *
 * author: 2744544
 *
 */

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Arrays;

//Direct import of the classes CachedRowSet and CachedRowSetImpl will fail because
//these classes are not exported by the module. Instead, one needs to import
//javax.sql.rowset.* as above.
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;



public class RecordsDatabaseService extends Thread{

    private Socket serviceSocket = null;
    private String[] requestStr  = new String[2]; //One slot for artist's name and one for record shop's name.
    private CachedRowSet outcome   = null;

	//JDBC connection
    private String USERNAME = fsd.week1.layoutgrid.Credentials.USERNAME;
    private String PASSWORD = fsd.week1.layoutgrid.Credentials.PASSWORD;
    private String URL      = fsd.week1.layoutgrid.Credentials.URL;



    //Class constructor
    public RecordsDatabaseService(Socket aSocket){
        serviceSocket = aSocket;
        this.start();
    }

    public String[] retrieveRequest() {
        this.requestStr[0] = "";
        this.requestStr[1] = "";
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(serviceSocket.getInputStream(), StandardCharsets.UTF_8));
            String message = reader.readLine(); // Read a line of text from the client
            if (message != null) {
                // Get rid of '#' and split message by ';'
                message = message.replace("#", "");
                this.requestStr = message.split(";"); // Assign values to the outer requestStr

                if (this.requestStr.length == 2) {
                    // Artist's surname is at index 0, Record shop city is at index 1
                    return this.requestStr;
                } else {
                    System.out.println("Service thread " + this.getId() + ": Invalid message format");
                }
            }
            reader.close();

        } catch (IOException e) {
            System.out.println("Service thread " + this.getId() + ": Error reading request: " + e.getMessage());
        }
        return null; // Return null to indicate an error
    }


    //Parse the request command and execute the query
    public boolean attendRequest()
    {
        boolean flagRequestAttended = true;
		
		this.outcome = null;

		String user = this.USERNAME;
        String password = this.PASSWORD;
		try {
			//Connect to the database
            Class.forName("org.postgresql.Driver"); // check
            Connection con = DriverManager.getConnection(URL, user, password);
			String sqlQuery = "SELECT r.title, r.label, r.genre, r.rrp, count(rc.copyid) FROM artist a JOIN record r ON a.artistid = r.artistid JOIN recordcopy rc ON rc.recordid = r.recordid JOIN recordshop rs ON rs.recordshopid = rc.recordshopid where a.lastname = ? and rs.city = ? group by title, label, genre, rrp"; // Need to modify SQL to be correct later
            PreparedStatement pstmt = con.prepareStatement(sqlQuery);
            pstmt.setString(1, requestStr[0]);
            pstmt.setString(2, requestStr[1]);
            ResultSet resultSet = pstmt.executeQuery();
            // Create CachedRowSet to hold query result
            RowSetFactory rowSetFactory = RowSetProvider.newFactory();
            CachedRowSet cachedRowSet = rowSetFactory.createCachedRowSet();

            // Transfer query result to CachedRowSet
            cachedRowSet.populate(resultSet);
            this.outcome = cachedRowSet;
            // Print Statement
            CachedRowSet crs = cachedRowSet;
            while (crs.next()) {
                String title = crs.getString(1);
                String label = crs.getString(2);
                String genre = crs.getString(3);
                String rrp = crs.getString(4);
                String copyId = crs.getString(5);
                System.out.println(title + " | " + label + " | " + genre + " | " + rrp + " | " + copyId);
            }
            resultSet.close();
		} catch (Exception e)
		{ System.out.println(e); flagRequestAttended = false; }

        return flagRequestAttended;
    }



    //Wrap and return service outcome
    public synchronized void returnServiceOutcome() {
        try {
			// Create an ObjectOutputStream with the OutputStream of the clientSocket
            ObjectOutputStream outcomeStreamWriter = new ObjectOutputStream(serviceSocket.getOutputStream());

            // Write the outcome (CachedRowSet) to the ObjectOutputStream
            outcomeStreamWriter.writeObject(this.outcome);
            outcomeStreamWriter.flush(); // Return outcome
    
            // Close the ObjectOutputStream (this will also flush the stream)
            System.out.println("Service thread " + this.getId() + ": Service outcome returned; " + this.outcome);
            serviceSocket.close(); //terminating connection
            outcomeStreamWriter.close();
			
        }catch (IOException e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
    }


    //The service thread run() method
    public void run()
    {
		try {
			System.out.println("\n============================================\n");
            //Retrieve the service request from the socket
            String[] request = this.retrieveRequest();
            System.out.println("Service thread " + this.getId() + ": Request retrieved: "
						+ "artist->" + this.requestStr[0] + "; recordshop->" + this.requestStr[1]);

            //Attend the request
            boolean tmp = this.attendRequest();

            //Send back the outcome of the request
            if (!tmp)
                System.out.println("Service thread " + this.getId() + ": Unable to provide service.");
            this.returnServiceOutcome();

        }catch (Exception e){
            System.out.println("Service thread " + this.getId() + ": " + e);
        }
        //Terminate service thread (by exiting run() method)
        System.out.println("Service thread " + this.getId() + ": Finished service.");
    }

}
