import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.Arrays;

public class Hw {
    private static String host, port, database, username, password;
    public static void main(String args[]) {
        try {
            Connection connection = getConnectionFromFile(args[0]);
            String queryNumber = args[1];
            String[] queryArguments = Arrays.copyOfRange(args,2,args.length);
            switch (queryNumber) {
                case "q1":
                    q1_rangeQuery(connection, queryArguments);
                    break;
                case "q2":
                    q2_pointQuery(connection, queryArguments);
                    break;
                case "q3":
                    q3_findSquadQuery(connection, queryArguments);
                    break;
                case "q4":
                    q4_routeCoverageQuery(connection, queryArguments);
                    break;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /*
    This method cretaes connection from the db.properties file specified in the argument
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

    public static void q1_rangeQuery(Connection connection,String queryArguments[]){

        int vehicleCount = Integer.parseInt(queryArguments[0]);
        String coordinates=null;
        String firstPoint = queryArguments[1] + " " +queryArguments[2];
        int index = 1; // column index for start of coordinates
        for (int i = 0; i < vehicleCount; i++) {
            if (coordinates != null) {
                coordinates = coordinates + queryArguments[index] + " " + queryArguments[index + 1];
            } else {
                coordinates = queryArguments[index] + " " + queryArguments[index + 1];
            }
            if (i != (vehicleCount - 1)) {
                coordinates = coordinates + ",";
            } else {
                coordinates = coordinates + "," + firstPoint;
            }
            index = index + 2;
        }
        String polygon = coordinates;
        String rangeQuery ="SELECT incidentId,incidentType, ST_X(incidentLocation) as X , ST_Y(incidentLocation) as Y " +
                "FROM incident  " +
                "WHERE ST_CONTAINS(ST_GEOMFROMTEXT('POLYGON(( "+polygon+" ))'),incidentLocation) " +
                "ORDER BY incidentId" ;

        try {
            PreparedStatement pstmt = connection.prepareStatement(rangeQuery);
            //pstmt.setString(1,polygon);
            ResultSet rs = pstmt.executeQuery();

            int countRow=0;
            while(rs.next()){
                int incidentId = rs.getInt(1);
                String incidentType = rs.getString(2);
                String incidentLocation = rs.getString(3) + ","+rs.getString(4);
                System.out.println(incidentId + " \t" +incidentLocation + " \t"+incidentType);
                countRow++;
            }
            if(countRow ==0){
                System.out.println("no incidents found that occurred within the polygon");
            }
            else{
                System.out.println(countRow+" rows returned.");
            }
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in while running query1:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        }
    }

    private static void q2_pointQuery(Connection connection, String[] queryArguments) {
        int distance = Integer.parseInt(queryArguments[0]);
        int incidentId = Integer.parseInt(queryArguments[1]);

        String pointQuery = "select officer.badgeNum, ROUND(ST_Distance_Sphere(officer.currentLocation, incident.incidentLocation)) as distance, officer.officerName " +
                "FROM incident JOIN officer " +
                "WHERE ST_Distance_Sphere(officer.currentLocation, incident.incidentLocation) <= ? " +
                "AND incident.incidentId = ? ORDER BY distance ASC";
        try {
            PreparedStatement pstmt = connection.prepareStatement(pointQuery);
            pstmt.setInt(1,incidentId);
            pstmt.setInt(2,distance);
            ResultSet rs = pstmt.executeQuery();
            int countRow =0;
            while(rs.next()){
                int badgeNum = rs.getInt(1);
                int officerDistance = rs.getInt(2);
                String officerName = rs.getString(3);
                System.out.println(badgeNum + " \t" +officerDistance + "m \t"+officerName);
                countRow++;
            }
            if(countRow ==0){
                System.out.println("No officer found that is within the given distance of the incident");
            }
            else{
                System.out.println(countRow+" rows returned.");
            }
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in while running query1:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        }
    }


    public static void q3_findSquadQuery(Connection connection, String queryArguments[]){
        int squadNumber = Integer.parseInt(queryArguments[0]);

        String findzoneName = "SELECT zone.zoneName FROM zone WHERE squadNumber =?";
        try {
            PreparedStatement pstmt = connection.prepareStatement(findzoneName);
            pstmt.setInt(1,squadNumber);
            ResultSet rs = pstmt.executeQuery();
            String zoneName = null;
            int countRow =0;
            while(rs.next()){
                zoneName = rs.getString(1);
                countRow++;
            }
            if(countRow >0){
                System.out.println("Squad "+ squadNumber +" is now patrolling: "+ zoneName );
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        String findSquadQuery = "select officer.badgeNum, officer.officerName, " +
                "CASE WHEN ST_CONTAINS(coordinates, currentLocation) =1 THEN 'IN' " +
                "WHEN ST_CONTAINS(coordinates, currentLocation) =0 THEN 'OUT' " +
                "END AS position " +
                "FROM officer JOIN zone ON officer.SquadNum = zone.squadNumber " +
                "WHERE zone.squadNumber = ? ORDER BY officer.badgeNum ";

        try {
            PreparedStatement pstmt = connection.prepareStatement(findSquadQuery);
            pstmt.setInt(1,squadNumber);
            ResultSet rs = pstmt.executeQuery();
            int countRow =0;
            while(rs.next()){
                int badgeNumber = rs.getInt(1);
                String officerName = rs.getString(2);
                String currentPosition = rs.getString(3);
                System.out.println(badgeNumber + " \t" +currentPosition + "\t \t"+officerName);
                countRow++;
            }
            if(countRow ==0){
                System.out.println("no officer assigned to the given squad.");
            } else{
                System.out.println(countRow+" rows returned.");
            }
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in while running query1:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        }
    }

    private static void q4_routeCoverageQuery(Connection connection, String[] queryArguments) {
        int routeNumber = Integer.parseInt(queryArguments[0]);
        String routeCoverageQuery= "SELECT zoneId, zoneName " +
                "FROM zone JOIN route " +
                "WHERE st_intersects(zone.coordinates,route.coordinates) AND route.routeNum = ? " +
                "ORDER BY zone.zoneId ASC";
        try {
            PreparedStatement pstmt = connection.prepareStatement(routeCoverageQuery);
            pstmt.setInt(1,routeNumber);
            ResultSet rs = pstmt.executeQuery();
            int countRow=0;
            while(rs.next()){
                int zoneId = rs.getInt(1);
                String zoneName = rs.getString(2);
                System.out.println(zoneId + " \t" +zoneName);
                countRow++;
            }
            if(countRow==0){
                System.out.println("there is no zone that the given patrol route passes through.");
            }else{
                System.out.println(countRow + " rows returned.");
            }
        } catch (SQLException SQLex) {
            System.out.println("SQL Exception in while running query1:");
            System.out.println("SQLException: " + SQLex.getMessage());
            System.out.println("SQLState: " + SQLex.getSQLState());
            System.out.println("VendorError: " + SQLex.getErrorCode());
        }
    }
}
