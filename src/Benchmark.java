import java.sql.*;
import java.util.Random;

public class Benchmark {

    Connection conn;

    public Benchmark(String ip) {
        try {
            if(ip.isEmpty()) {
                conn = connect("127.0.0.1");
            }
            else {
                conn=connect(ip);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    protected static Connection getConnection(String url, String user, String pwd) throws SQLException {
        return DriverManager.getConnection(url, user, pwd);
    }

    public static Connection connect(String targetIp) {
        String url = "jdbc:mysql://"+targetIp+":3306/benchdb";
        String user = "dbi";
        String password = "dbi_pass";
        Connection conn = null;

        try {
            conn = getConnection(url, user, password);
            System.out.println("Connection established");
            System.out.println("");
        }
        catch(SQLException ex){
            System.err.println(ex.getMessage());
        }
        return conn;
    }

    public static void initDb(Connection conn, int n) throws SQLException {
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        String sqlDropHistoryIfExists = "TRUNCATE table history;";
        stmt.executeUpdate(sqlDropHistoryIfExists);
        conn.commit();
        System.out.println("Dropped History values");
        try {
            /*conn.setAutoCommit(false);
            Statement stmt = conn.createStatement();*/
            String sqlDropSchemaIfExists = "DROP schema if exists benchdb;";
            stmt.executeUpdate(sqlDropSchemaIfExists);
            System.out.println("\nDrop schema if exists\n");

            //erzeuge das schema benchdb
            String sqlCreateSchema = "create schema benchdb;";
            stmt.executeUpdate(sqlCreateSchema);
            System.out.println("\nCreated schema\n");

            System.out.println("\nConnected to benchmark database!\n");

            //erzeuge Tabelle 'branches' in dem schema 'benchdb'
            String sqlBranches =
                    "create table `benchdb`.branches\n" +
                            "( branchid int not null,\n" +
                            " branchname char(20) not null,\n" +
                            " balance int not null,\n" +
                            " address char(72) not null,\n" +
                            " primary key (branchid) );";
            stmt.executeUpdate(sqlBranches);
            System.out.println("\nCreated Table branches\n");


            //erzeuge Tabelle 'accounts' in dem schema 'benchdb'

            String sqlAccounts =
                    "create table `benchdb`.accounts\n" +
                            "( accid int not null,\n" +
                            " name char(20) not null,\n" +
                            " balance int not null,\n" +
                            "branchid int not null,\n" +
                            "address char(68) not null,\n" +
                            "primary key (accid),\n" +
                            "foreign key (branchid) references `benchdb`.branches(branchid) );";
            stmt.executeUpdate(sqlAccounts);
            System.out.println("\nCreated Table accounts\n");

            //erzeuge Tabelle 'tellers' in dem schema 'benchdb'
            String sqlTellers =
                    "create table `benchdb`.tellers\n" +
                            "( tellerid int not null,\n" +
                            " tellername char(20) not null,\n" +
                            " balance int not null,\n" +
                            " branchid int not null,\n" +
                            " address char(68) not null,\n" +
                            " primary key (tellerid),\n" +
                            " foreign key (branchid) references `benchdb`.branches(branchid) ); ";
            stmt.executeUpdate(sqlTellers);
            System.out.println("\nCreated Table tellers\n");

            //erzeuge Tabelle 'history' in dem schema 'benchdb'

            String sqlHistory =
                    "create table `benchdb`.history\n" +
                            "( accid int not null,\n" +
                            " tellerid int not null,\n" +
                            " delta int not null,\n" +
                            " branchid int not null,\n" +
                            " accbalance int not null,\n" +
                            " cmmnt char(30) not null,\n" +
                            " foreign key (accid) references `benchdb`.accounts(accid),\n" +
                            " foreign key (tellerid) references `benchdb`.tellers(tellerid),\n" +
                            " foreign key (branchid) references `benchdb`.branches(branchid) );";
            stmt.executeUpdate(sqlHistory);
            System.out.println("\nCreated Table history\n");


            String SQL_Insert_branches = "insert into `benchdb`.branches(branchid, branchname, balance, address) values (?,?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(SQL_Insert_branches);
            for (int i = 1; i <= n; i++) {
                preparedStatement.setInt(1, i);
                preparedStatement.setString(2, "ABCDEFGHIJKLMNOPQRST");
                preparedStatement.setInt(3, 0);
                preparedStatement.setString(4, "ABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQR");
                preparedStatement.executeUpdate();
            }

            String SQL_Insert_accounts = "insert into `benchdb`.accounts(accid, balance, branchid, name, address) values (?,?,?,?,?)";
            preparedStatement = conn.prepareStatement(SQL_Insert_accounts);
            for (int i = 1; i <= n * 100000; ++i) {
                Random rand = new Random();
                int randomBranchid = rand.nextInt(n) + 1;
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, 0);
                preparedStatement.setInt(3, randomBranchid);
                preparedStatement.setString(4, "ABCDEFGHIJKLMNOPQRST");
                preparedStatement.setString(5, "ABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQR");
                preparedStatement.addBatch();
                if(i % 100000 == 0) {

                    preparedStatement.executeBatch();
                }
                if( i% 1000000== 0){
                    System.out.println("1000000");
                }

            }
            System.out.println("ready");

            String SQL_Insert_tellers = "insert into `benchdb`.tellers(tellerid, balance, branchid, tellername, address) values (?,?,?,?,?) ";
            preparedStatement = conn.prepareStatement(SQL_Insert_tellers);
            for (int i = 1; i <= n * 10; ++i) {
                Random rand = new Random();
                int randomBranchid = rand.nextInt(n) + 1;
                preparedStatement.setInt(1, i);
                preparedStatement.setInt(2, 0);
                preparedStatement.setInt(3, randomBranchid);
                preparedStatement.setString(4, "ABCDEFGHIJKLMNOPQRST");
                preparedStatement.setString(5, "ABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQRSTUVXYZABCDEFGHIJKLMNOPQR");
                preparedStatement.executeUpdate();
            }
            conn.commit();
            preparedStatement.close();

    } catch (SQLException e) {
        System.err.println(e.toString());
        System.exit(1);
    }
    }

    public static int kontostands_TX(int accid, Connection conn){
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT accounts.balance FROM accounts WHERE accounts.accid = " + accid + ";");

            rs.next();
            //int balance = rs.getInt(1);
            //return balance;
            return rs.getInt(1);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public static int einzahlungs_TX(int accid, int tellerid, int branchid, int delta, Connection conn) {
        try {
            Statement stmt = conn.createStatement();

            stmt.executeUpdate("UPDATE branches SET branches.balance = (branches.balance + " + delta + ") WHERE branches.branchid = " + branchid);
            stmt.executeUpdate("UPDATE tellers SET tellers.balance = (tellers.balance + " + delta + ") WHERE tellers.tellerid = " + tellerid);
            stmt.executeUpdate("UPDATE accounts SET accounts.balance = (accounts.balance + " + delta + ") WHERE accounts.accid = " + accid);
            stmt.executeUpdate("INSERT INTO history VALUES(" + accid + ", " + tellerid + ", " + delta + ", " + branchid + ", " + kontostands_TX(accid, conn) + ", 'abcdefghijklmnopqrstuvwxyzabcd');");

            return kontostands_TX(accid, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }

    public static int analyse_TX(int delta, Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(history.accid) FROM history where history.delta = " + delta);

            rs.next();
           // int numberOfPayments = rs.getInt(1);
           // return numberOfPayments;
            return rs.getInt(1);

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }






    public static int einzahlungs_TXv2(int accid, int tellerid, int branchid, int delta, Connection conn) {
        try {
            Statement stmt = conn.createStatement();

            stmt.executeQuery("SELECT einzahlungsTX('" + accid + "', '" + tellerid + "', '" + branchid + "', '" + delta + "')");

            return kontostands_TX(accid, conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }
    public static int kontostands_TXv2(int accid, Connection conn){
        try {
            Statement stmt = conn.createStatement();

            stmt.executeQuery("SELECT kontostandsTX('" + accid + "')");

            return 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }

    /**
     * This function returns the count of transactions by a specified amount by using a preexisting function in the database
     * @param delta specified amount
     * @param conn Connection to the database
     * @return count of transactions
     */
    public static int analyse_TXv2(int delta, Connection conn) {
        try {
            Statement stmt = conn.createStatement();

            stmt.executeQuery("SELECT analyseTX('" +  delta + "')");

            return 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return 0;
    }



}

