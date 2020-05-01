package nosql.benchmark;

import java.sql.*;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.stream.Collectors;


public class Benchmark {
    private final String url;
    private final String usr;
    private final String pwd;
    private Connection con = null;
    private EnumMap<BenchmarkTyp, ArrayList<Long>> benchmarkResults;

    int N = 3000;

    public static void main(String args[]) {
        //System.out.println("mysql");

        //new Benchmark("jdbc:mysql://localhost:3306/test", "root", "mysql").start();
		System.out.println("monetdb");
		new Benchmark("jdbc:monetdb://@localhost:50000/db", "monetdb", "monetdb").start();

    }

    public Benchmark(String url, String usr, String pwd) {
        // Datenbankverbindung oeffnen
        benchmarkResults = new EnumMap<BenchmarkTyp, ArrayList<Long>>(BenchmarkTyp.class);
        this.url = url;
        this.usr = usr;
        this.pwd = pwd;

    }

    private void start() {
        int bn = 20;
        System.out.printf("N=%d\n", N);
        System.out.printf("Running %d Benchmarks\n", bn);
        for (int i = 0; i < bn; i++) {
            try {
                con = DriverManager.getConnection(url, usr, pwd);
                con.setAutoCommit(true);
                //System.out.println("--- DROP ---");
                dropTable();
                //System.out.println("--- CREATE ---");
                createTable();
                //System.out.println("--- INSERT ---");
                insertTable();
                //System.out.println("--- SELECT ---");
                selectPerson();

                //Datenbankverbindung schliessn
                con.close();

            } catch (SQLException ex) {
                printSQLException(ex);
            }
        }

        printBenchmarkResults();

    }


    // Schema loeschen
    private void dropTable() {
        long time = System.currentTimeMillis();
        try {
//				SQL-Statement anpassen
            String sqlString = "DROP TABLE IF EXISTS person";
            Statement stmt = con.createStatement();
            int anzahl = stmt.executeUpdate(sqlString);
            stmt.close();
        } catch (SQLException e) {
            printSQLException(e);
        }
        //System.out.println("Zeit Drop Tabelle " + (System.currentTimeMillis() - time));
        updateBenchmarkResult(BenchmarkTyp.DROP, time);
    }

    // CREATE TABLE
    private void createTable() {
        long time = System.currentTimeMillis();

        try {
//				SQL-Statement anpassen
            String sqlString = "CREATE TABLE person(id integer primary key)";
            Statement stmt = con.createStatement();
            int anzahl = stmt.executeUpdate(sqlString);
            stmt.close();
        } catch (SQLException e) {
            printSQLException(e);
        }
        updateBenchmarkResult(BenchmarkTyp.CREATE, time);
        //System.out.println("Zeit Create Tabelle "+(System.currentTimeMillis()-time));
    }

    // Insert durchfuehren
    private void insertTable() {
        long time = System.currentTimeMillis();

        try {
//				SQL-Statement anpassen
            String sqlString = "INSERT INTO person VALUES (?)";
            PreparedStatement stmt = con.prepareStatement(sqlString);

            for (int i = 1; i < N; i++) {
                stmt.setInt(1, i);
                int anzahl = stmt.executeUpdate();
            }
            stmt.close();
        } catch (SQLException e) {
            printSQLException(e);
        }
        //System.out.println("Zeit INSERT (N="+N+"): "+(System.currentTimeMillis()-time));
        updateBenchmarkResult(BenchmarkTyp.INSERT, time);
    }


    private void updateBenchmarkResult(BenchmarkTyp key, long time){
        benchmarkResults.computeIfAbsent(key, k -> new ArrayList<>());
        var list = benchmarkResults.get(key);
        list.add((System.currentTimeMillis() - time));
    }

    private void printBenchmarkResults(){
//        var types = BenchmarkTyp.values();
        for(var type: BenchmarkTyp.values()){
            ArrayList<Long> values = this.benchmarkResults.get(type);

            var average = values.stream().collect(Collectors.summarizingLong(Long::longValue)).getAverage();
            System.out.printf("Operation: %s average %.1f\nms", type, average);
        }
    }

    // SELECT
    private void selectPerson() {
        long time = System.currentTimeMillis();
//		SQL-Statement anpassen
        String sqlString = "Select id from person";

        try {
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(sqlString);
            while (rs.next()) {
                Integer pid = rs.getInt(1);
            }
            stmt.close();
        } catch (SQLException e) {
            printSQLException(e);
        }
        //System.out.println("Zeit Select (N=" + N + "): " + (System.currentTimeMillis() - time));
        updateBenchmarkResult(BenchmarkTyp.SELECT, time);

    }


    private void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                System.out.println("\n---SQLException---\n");
                SQLException sqlex = (SQLException) e;
                System.err.println("Message: " + sqlex.getMessage());
                System.err.println("SQLState: " + sqlex.getSQLState());
                System.err.println("Error Code: " + sqlex.getErrorCode());
                System.out.println("");
            }

            if (e instanceof SQLWarning) {
                System.out.println("\n---SQLWarning---\n");
                SQLException sqlw = (SQLWarning) e;
                System.err.println("Message: " + sqlw.getMessage());
                System.out.println("SQLState: " + sqlw.getSQLState());
                System.out.print("Vendor error code: " + sqlw.getErrorCode());
                System.out.println("");
                //warning = warning.getNextWarning();
            }
        }
        ex.printStackTrace(System.err);
    }
}

