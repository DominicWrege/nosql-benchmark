package nosql.benchmark;

import java.sql.*;

     
public class Benchmark {
	private final String url;
	private final String usr;
	private final String pwd;
	Connection con = null;

	int N=1000;

	public static void main(String args[]) {
		System.out.println("N=1000");
		System.out.println("running mysql");

		new Benchmark("jdbc:mysql://172.22.160.87:3306/test", "root", "mysql").start();
		System.out.println("running monetdb");
		new Benchmark("jdbc:monetdb://172.22.160.87:50000/db", "monetdb", "monetdb").start();

	}
	public Benchmark(String url, String usr, String pwd) {
		// Datenbankverbindung oeffnen
		this.url = url;
		this.usr = usr;
		this.pwd = pwd;

	}

	private void start(){
		try {
			con = DriverManager.getConnection(url,usr, pwd);
			con.setAutoCommit(true);
			System.out.println("--- DROP ---");
			dropTable();
			System.out.println("--- CREATE ---");
			createTable();
			System.out.println("--- INSERT ---");
			insertTable();
			System.out.println("--- SELECT ---");
			selectPerson();

			//Datenbankverbindung schliessn
			con.close();

		} catch(SQLException ex) {
			printSQLException(ex);
		}
	}
	

	// Schema loeschen 
		private void dropTable(){
			long time=System.currentTimeMillis();
			try{
//				SQL-Statement anpassen
				String sqlString="DROP TABLE IF EXISTS person";
				Statement stmt = con.createStatement();
				int anzahl = stmt.executeUpdate(sqlString);
				stmt.close();
			}catch(SQLException e){
				printSQLException(e);
			}
			System.out.println("Zeit Drop Tabelle "+(System.currentTimeMillis()-time));
		}
	
		// CREATE TABLE 
		private void createTable(){
			long time=System.currentTimeMillis();
			
			try{
//				SQL-Statement anpassen
				String sqlString="CREATE TABLE person(id integer primary key)";
				Statement stmt = con.createStatement();
				int anzahl = stmt.executeUpdate(sqlString);
				stmt.close();
			}catch(SQLException e){
				printSQLException(e);
			}
			System.out.println("Zeit Create Tabelle "+(System.currentTimeMillis()-time));
		}

		// Insert durchfuehren
		private void insertTable(){
			long time=System.currentTimeMillis();
			
			try{
//				SQL-Statement anpassen
				String sqlString="INSERT INTO person VALUES (?)";
				PreparedStatement stmt = con.prepareStatement(sqlString);
				
				for (int i=1;i<N;i++) {
					stmt.setInt(1,i);
					int anzahl = stmt.executeUpdate();
				}
				stmt.close();
			}catch(SQLException e){
				printSQLException(e);
			}
			System.out.println("Zeit INSERT (N="+N+"): "+(System.currentTimeMillis()-time));
		}

	// SELECT
	private void selectPerson(){
		long time = System.currentTimeMillis();
//		SQL-Statement anpassen
		String sqlString="Select id from person";
		
		try{
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery(sqlString);
				while (rs.next()){
					Integer pid = rs.getInt(1);
				}
			stmt.close();
		}catch(SQLException e){
			printSQLException(e);
		}
		System.out.println("Zeit Select (N="+N+"): "+(System.currentTimeMillis()-time));
	}
	

	
	private void printSQLException(SQLException ex) {
	    for (Throwable e : ex) {
	        if (e instanceof SQLException) {
	        	System.out.println("\n---SQLException---\n");
	            SQLException sqlex = (SQLException) e;
	            System.err.println("Message: " +sqlex.getMessage());
                System.err.println("SQLState: "+sqlex.getSQLState());
                System.err.println("Error Code: " +sqlex.getErrorCode());
                System.out.println("");
	        }

	        if (e instanceof SQLWarning) {
	            System.out.println("\n---SQLWarning---\n");
	            SQLException sqlw = (SQLWarning) e;
	            System.err.println("Message: " +sqlw.getMessage());
	            System.out.println("SQLState: " + sqlw.getSQLState());
	            System.out.print("Vendor error code: " + sqlw.getErrorCode());
	            System.out.println("");
	            //warning = warning.getNextWarning();
	        }
	    }
        ex.printStackTrace(System.err);
	}
}

