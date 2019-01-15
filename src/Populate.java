import java.io.*;
import java.sql.*;

public class Populate {
    private static String host, port, database, username, password;

    public static void main(String args[]) throws IOException {

       String connectionFilename = args[0];
        Statement stmt = null;
        Connection con = null;
        try {
            con = getConnectionFromFile(connectionFilename);
            if(con == null){
                System.out.println("could not establish connection successfully! Try again.");
            }
            else {
                stmt = con.createStatement();
                populateZone(stmt,args[1]);
                populateOfficer(stmt,args[2]);
                populateRoute(stmt, args[3]);
                populateIncident(stmt, args[4]);
            }
        }
        catch (SQLException SQLex) {
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        } finally {
            //finally block used to close resources
            if (stmt != null) {
                try {
                    con.close();
                    System.out.println("connection closed successfully!");
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /*
    This method establishes connection with the database.
     */
    public static Connection getConnectionFromFile(String filename) throws SQLException {
        BufferedReader bufferedReader;
        try {
            bufferedReader = new BufferedReader(new FileReader(filename));
            host = bufferedReader.readLine();
            port = bufferedReader.readLine();
            database = bufferedReader.readLine();
            username = bufferedReader.readLine();
            password = bufferedReader.readLine();
        } catch (FileNotFoundException e) {
            System.out.println("Check if the file exists in the current working directory.");
            System.out.println(e.getMessage());
        } catch (IOException IOex) {
            System.out.println("file I/O exception occurred. ");
            System.out.println("IO Exception:\n"+IOex.getMessage());
        }
        // Class.forName("com.mysql.jdbc.Driver");
        String connectionURL = "jdbc:mysql://" + host + ":" + port + "/" + database;
        return DriverManager.getConnection(connectionURL, username, password);
    }

    /*
    This method inserts data into Zone table from file specified in the arguments.
    */
    public static void populateZone(Statement stmt, String filename) {
        try {
            ResultSet rs = stmt.executeQuery("select * from zone");
            if (rs != null) {
                //for deleting already excisting data in the table
                String deleteFromZone = "TRUNCATE TABLE " + database + ".zone";
                stmt.executeUpdate(deleteFromZone);
            }
            BufferedReader bufferedReaderZone = new BufferedReader(new FileReader(filename));

            String zonedata;
            int rowinserted = 0;
            while ((zonedata = bufferedReaderZone.readLine()) != null) {
                int zoneId, squadNumber, verticesCount;
                String zoneName, coordinates = null;

                String[] zoneValues = zonedata.split(", ");

                zoneId = Integer.parseInt(zoneValues[0]);
                zoneName = zoneValues[1];
                squadNumber = Integer.parseInt(zoneValues[2]);
                verticesCount = Integer.parseInt(zoneValues[3]);
                String firstPoint = "0 0";
                int index = 4; // column index for start of coordinates
                for (int i = 0; i < verticesCount; i++) {
                    if (coordinates != null) {
                        coordinates = coordinates + zoneValues[index] + " " + zoneValues[index + 1];
                    } else {
                        coordinates = zoneValues[index] + " " + zoneValues[index + 1];
                        firstPoint = coordinates;
                    }
                    if (i != (verticesCount - 1)) {
                        coordinates = coordinates + ",";
                    } else {
                        coordinates = coordinates + "," + firstPoint;
                    }
                    index = index + 2;
                }
                String insertInZone = "INSERT INTO " + database + ".zone " +
                        "VALUES (" + zoneId + "," + zoneName + "," + squadNumber + "," + verticesCount + "," +
                        "ST_GeomFromText('POLYGON((" + coordinates + "))\'))";
                stmt.executeUpdate(insertInZone);
                rowinserted++;
            }
            System.out.println(rowinserted +" rows inserted in the Zone table successfully.");
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in Zone table:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException occured. Check if the file exists in the current working directory.");
            System.out.println(e.getMessage());
        } catch (IOException IOex) {
            System.out.println("file I/O exception occurred. ");
            System.out.println("IO Exception:\n"+IOex.getMessage());
        }
    }


    /*
    This method inserts data into Officer table from file specified in the arguments.
    */
    public static void populateOfficer(Statement stmt, String filename) {
        try {
            ResultSet rs = stmt.executeQuery("select * from officer");
            if (rs != null) {
                //for deleting already excisting data in the table
                String deleteFromZone = "TRUNCATE TABLE " + database + ".officer";
                stmt.executeUpdate(deleteFromZone);
            }
            BufferedReader bufferedReaderZone = new BufferedReader(new FileReader(filename));

            String officerdata;

            int rowinserted =0;
            while ((officerdata = bufferedReaderZone.readLine()) != null) {
                int badgeNum, squadNumber;
                String officerName, currentLocation;
                String[] officerValues = officerdata.split(", ");
                badgeNum = Integer.parseInt(officerValues[0]);
                officerName = officerValues[1];
                squadNumber = Integer.parseInt(officerValues[2]);
                currentLocation = officerValues[3] + " " +officerValues[4];

                String insertInOfficer = "INSERT INTO " + database + ".officer " +
                        "VALUES (" + badgeNum + "," + officerName + "," + squadNumber + "," +
                        "ST_PointFromText('POINT(" + currentLocation + ")\'))";
                stmt.executeUpdate(insertInOfficer);
                rowinserted++;
            }
            System.out.println(rowinserted +" rows inserted in the Officer table successfully.");
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in Officer table:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException occured. Check if the file exists in the current working directory.");
            System.out.println(e.getMessage());
        } catch (IOException IOex) {
            System.out.println("file I/O exception occurred. ");
            System.out.println("IO Exception:\n"+IOex.getMessage());
        }
    }


    /*
    This method inserts data into Route table from file specified in the arguments.
    */
    public static void populateRoute(Statement stmt, String filename) {
        try {
            ResultSet rs = stmt.executeQuery("select * from route");
            if (rs != null) {
                //for deleting already excisting data in the table
                String deleteFromZone = "TRUNCATE TABLE " + database + ".route";
                stmt.executeUpdate(deleteFromZone);
            }
            BufferedReader bufferedReaderZone = new BufferedReader(new FileReader(filename));
            String routedata;

            int rowinserted=0;
            while ((routedata = bufferedReaderZone.readLine()) != null) {
                int routeNumber, vehicleCount;
                String coordinates = null;
                String[] routeValues = routedata.split(", ");
                routeNumber = Integer.parseInt(routeValues[0]);
                vehicleCount = Integer.parseInt(routeValues[1]);

                int index = 2; // column index for start of coordinates
                for (int i = 0; i < vehicleCount; i++) {
                    if (coordinates != null) {
                        coordinates = coordinates + routeValues[index] + " " + routeValues[index + 1];
                    } else {
                        coordinates = routeValues[index] + " " + routeValues[index + 1];
                    }
                    if (i != (vehicleCount - 1)) {
                        coordinates = coordinates + ",";
                    }
                    index = index + 2;
                }
                String insertInZone = "INSERT INTO " + database + ".route " +
                        "VALUES (" + routeNumber + "," + vehicleCount + "," +
                        "ST_LineStringFromText('LINESTRING(" + coordinates + ")\'))";

                stmt.executeUpdate(insertInZone);
                rowinserted++;
            }
            System.out.println(rowinserted +" rows inserted in the Route table successfully.");

        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in Route table:");
            //TODO : handle more such exception
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException occured. Check if the file exists in the current working directory.");
            System.out.println(e.getMessage());
        } catch (IOException IOex) {
            System.out.println("file I/O exception occurred. ");
            System.out.println("IO Exception:\n"+IOex.getMessage());
        }
    }

    /*
    This method inserts data into Incident table from file specified in the arguments.
    */
    public static void populateIncident(Statement stmt, String filename) {
        try {
            ResultSet rs = stmt.executeQuery("select * from incident");
            if (rs != null) {
                // for deleting already excisting data in the table
                String deleteFromZone = "TRUNCATE TABLE " + database + ".incident";
                stmt.executeUpdate(deleteFromZone);
            }
            BufferedReader bufferedReaderZone = new BufferedReader(new FileReader(filename));
            String incidentdata;

            int rowinserted =0;
            while ((incidentdata = bufferedReaderZone.readLine()) != null) {
                int incidentId;
                String incidentType, incidentLocation;
                String[] incidentValues = incidentdata.split(", ");
                incidentId = Integer.parseInt(incidentValues[0]);
                incidentType = incidentValues[1];
                incidentLocation = incidentValues[2] + " " +incidentValues[3];
                String insertInOfficer = "INSERT INTO " + database + ".incident " +
                        "VALUES (" + incidentId + "," + incidentType + "," +
                        "ST_PointFromText('POINT(" + incidentLocation + ")\'))";
                stmt.executeUpdate(insertInOfficer);
                rowinserted++;
            }
            System.out.println(rowinserted +" rows inserted in the incident table successfully.");
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in Incident table:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        } catch (FileNotFoundException e) {
            System.out.println("FileNotFoundException occured. Check if the file exists in the current working directory.");
            System.out.println(e.getMessage());
        } catch (IOException IOex) {
            System.out.println("file I/O exception occurred. ");
            System.out.println("IO Exception:\n"+IOex.getMessage());
        }
    }
}
