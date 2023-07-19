package miniProject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class Database {
  
  

  // NOTE: You will need to change some variables from START to END.
  public static void dropTable(Connection connection, String table) {
    Statement st = null;
    try {
      st = connection.createStatement();
      st.execute("DROP TABLE IF EXISTS " + table);
      st.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void createTable(Connection connection, String tableDescription) {
    Statement st = null;
    try {
      st = connection.createStatement();
      st.execute("CREATE TABLE " + tableDescription);
      st.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static int insertIntoTableFromFile(Connection connection, String table, String filename) {
    BufferedReader br = null;
    int numRows = 0;
    try {
      Statement st = connection.createStatement();
      String sCurrentLine, brokenLine[], composedLine = "";
      br = new BufferedReader(new FileReader(filename));

      while ((sCurrentLine = br.readLine()) != null) {
        // Insert each line to the DB
        brokenLine = sCurrentLine.split(",");
        composedLine = "INSERT INTO " + table + " VALUES (";
        int i;
        for (i = 0; i < brokenLine.length - 1; i++) {
          composedLine += "'" + brokenLine[i] + "',";
        }
        composedLine += "'" + brokenLine[i] + "')";
        numRows = st.executeUpdate(composedLine);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      try {
        if (br != null)
          br.close();
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return numRows;
  }


  public static ResultSet executeQuery(Connection connection, String query) {
    System.out.println("DEBUG: Executing query...");
    try {
      Statement st = connection.createStatement();
      ResultSet rs = st.executeQuery(query);
      return rs;
    } catch (SQLException e) {
      e.printStackTrace();
      return null;
    }
  }

  public static void main(String[] argv) throws SQLException {
    // START
    //Enter your username
    Scanner input = new Scanner(System.in);
    System.out.println("Enter your username");
    String user = input.nextLine();
    // Enter your database password, NOT your university password.
    System.out.println("Enter your password");
    String password = input.nextLine();
    input.close();

    /**
     * IMPORTANT: If you are using NoMachine, you can leave this as it is.
     * 
     * Otherwise, if you are using your OWN COMPUTER with TUNNELLING: 1) Delete the original
     * database string and 2) Remove the '//' in front of the second database string.
     */
    String database = "teachdb.cs.rhul.ac.uk";
    // END

    Connection connection = connectToDatabase(user, password, database);
    if (connection != null) {
      System.out
          .println("SUCCESS: You made it!" + "\n\t You can now take control of your database!\n");
    } else {
      System.out.println("ERROR: \tFailed to make connection!");
      System.exit(1);
    }
    // Now we're ready to use the DB. You may add your code below this line.



    dropTable(connection, "airport");

   createTable(connection,
   "airport (airport_Code varchar(10), airport_Name varchar(100), city varchar(30), state varchar(10), primary key (airport_code));");



    dropTable(connection, "delayed_flights");

    createTable(connection, "delayed_flights (ID_of_Delayed_Flight int, Month int, DayofMonth int, DayOfWeek int, DepTime int, ScheduledDepTime int, ArrTime int, ScheduledArrTime int, UniqueCarrier varchar(4) , FlightNum int, ActualFlightTime int, scheduledFlightTime int, AirTime int, ArrDelay int, DepDelay int, Orig varchar(10), Dest varchar(10), Distance int, primary key (ID_of_Delayed_Flight));");

    insertIntoTableFromFile(connection, "airport", "src/airport");
    insertIntoTableFromFile(connection, "delayed_flights", "src/delayedFlights");


    System.out.println("\n################## 1st Query ###############");

    // Execute the query to retrieve the top 5 carriers with the most delays
    Statement st = connection.createStatement();
    ResultSet rs =
        st.executeQuery("SELECT UniqueCarrier, COUNT(*) AS num_delays " + "FROM delayed_flights "
            + "GROUP BY UniqueCarrier " + "ORDER BY num_delays DESC " + "LIMIT 5");

    // Iterate over the ResultSet and print the results
    int i = 1;
    while (rs.next()) {
      // Get the values of the columns in the current row
      String carrier = rs.getString(1);
      int numDelays = rs.getInt(2);
      // Print the values
      System.out.println(carrier + " " + numDelays);
      i++;
    }

    // If there are fewer than 5 carriers, print an empty line to separate the queries
    if (i <= 5) {
      System.out.println();
    }

    // Close the ResultSet and Statement objects
    rs.close();
    st.close();

    System.out.println("\n################## 2nd Query ###############");


    // Execute the query and retrieve the ResultSet object
    st = connection.createStatement();
    rs = st.executeQuery("SELECT airport.city, COUNT(*) AS num_delays " + "FROM delayed_flights "
        + "INNER JOIN airport ON delayed_flights.Orig = airport.airport_Code "
        + "GROUP BY airport.city " + "ORDER BY num_delays DESC " + "LIMIT 5");

    // Iterate over the ResultSet and print the results
    i = 1;
    while (rs.next()) {
      // Get the values of the columns in the current row
      String city = rs.getString(1);
      int numDelays = rs.getInt(2);
      // Print the values
      System.out.println(city + " " + numDelays);
      i++;
    }

    // If there are fewer than 5 cities, print an empty line to separate the queries
    if (i <= 5) {
      System.out.println();
    }

    // Close the Statement and ResultSet object
    st.close();
    rs.close();

    System.out.println("\n################## 3rd Query ###############");

    st = connection.createStatement();
    rs = st
        .executeQuery("SELECT Dest, SUM(ArrDelay) AS total_arrival_delay_minutes "
            + "FROM delayed_flights " + "GROUP BY Dest "
            + "ORDER BY total_arrival_delay_minutes DESC " + "LIMIT 5 " + "OFFSET 1");

    // Iterate over the ResultSet and print the results
    while (rs.next()) {
      // Get the values of the columns in the current row
      String dest = rs.getString(1);
      int totalArrivalDelayMinutes = rs.getInt(2);
      // Print the values
      System.out.println(dest + " " + totalArrivalDelayMinutes);
    }

    // Close the ResultSet and Statement objects
    rs.close();
    st.close();
    
    System.out.println("\n################## 4th Query ###############");

     st = connection.createStatement();
     rs = st.executeQuery(
        "SELECT state, COUNT(*) AS num_airports " +
        "FROM airport " +
        "GROUP BY state " +
        "HAVING COUNT(*) >= 10 " +
        "ORDER BY num_airports DESC");
     
     // Iterate over the ResultSet and print the results
     while (rs.next()) {
       // Get the values of the columns in the current row
       String state = rs.getString(1);
       int numAirports = rs.getInt(2);
       // Print the values
       System.out.println(state + " " + numAirports);
     }
     
     // Close the ResultSet and Statement objects
     rs.close();
     st.close();
     
     System.out.println("\n################## 5th Query ###############");

     //Pretty sure query 5 doesn't work as it displays no results
     //However I am leaving the code in in case I can pick up some marks.

  // Execute the query to retrieve the top 5 states with the most delays within the state
       st = connection.createStatement();
       rs = st.executeQuery(
           "SELECT state, COUNT(*) AS num_delays " +
               "FROM delayed_flights d INNER JOIN airport a ON d.Orig = a.airport_Code " +
               "WHERE a.state = d.Dest " +
               "GROUP BY state " +
               "ORDER BY num_delays DESC " +
               "LIMIT 5");

     // Iterate over the ResultSet and print the results
      i = 1;
     while (rs.next()) {
       // Get the values of the columns in the current row
       String state = rs.getString(1);
       int numDelays = rs.getInt(2);
       // Print the values
       System.out.println(state + " " + numDelays);
       i++;
     }

     // If there are fewer than 5 states, print an empty line to separate the queries
     if (i <= 5) {
       System.out.println();
     }

     // Close the ResultSet and Statement objects
     rs.close();
     st.close();

  }



//ADVANCED: This method is for advanced users only. You should not need to change this!
  public static Connection connectToDatabase(String user, String password, String database) {
      System.out.println("------ Testing PostgreSQL JDBC Connection ------");
      Connection connection = null;
      try {
          String protocol = "jdbc:postgresql://";
          String dbName = "/CS2855/";
          String fullURL = protocol + database + dbName + user;
          connection = DriverManager.getConnection(fullURL, user, password);
      } catch (SQLException e) {
          String errorMsg = e.getMessage();
          if (errorMsg.contains("authentication failed")) {
              System.out.println("ERROR: \tDatabase password is incorrect. Have you changed the password string above?");
              System.out.println("\n\tMake sure you are NOT using your university password.\n"
                      + "\tYou need to use the password that was emailed to you!");
          } else {
              System.out.println("Connection failed! Check output console.");
              e.printStackTrace();
          }
      }
      return connection;
  }
}
