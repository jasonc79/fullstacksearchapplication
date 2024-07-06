package fsd.week1.layoutgrid;

public class Credentials {
    //JDBC connection
    public static final String USERNAME = "postgres";
    public static final String PASSWORD = "Fj78952gh!";
    public static final String URL = "jdbc:postgresql://localhost/FSAD2024_Records";
    //Client-server connection
    public static final String HOST = "172.30.96.1"; //localhost - IP Address
    public static final int PORT = 9994; //This is NOT the port in postgres, but the port at which the RecordsDatabaseServer is listening
}
