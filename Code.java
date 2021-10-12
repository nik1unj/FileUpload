import java.io.*;
import java.sql.*;

//NSR
public class nikunj 
{
	public static void main(String[] args) throws Exception
	{ 
		long batchInterval = 100000,row=0;
	  
		//Database Connectivity.
	    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
	    Connection connection =   DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe","system","nikunj12345");
	    String w[] = new String[5];
        BufferedReader r1 = new BufferedReader(new FileReader("C:\\Users\\Nikunj Ramani\\Desktop\\nikunj.txt")); //File name is nikunj.txt.
        String line = r1.readLine();
		
        //Query to insert and truncate the table.
        String sql = "insert into Demo (C1,C2,C3,C4,C5) values(?,?,?,?,?)";  //Demo is Database name where C1,C2,C3,C4,C5 is column name.
        String sql1="TRUNCATE table Demo";
		PreparedStatement statement = connection.prepareStatement(sql);
		PreparedStatement statement1 = connection.prepareStatement(sql1);
		statement1.executeQuery();
        
		long start = System.currentTimeMillis();
		while (line != null)
		{  
			w=line.split("\\|");	
            statement.setString(1, w[0]);
			statement.setString(2, w[1]);
            statement.setString(3, w[2]);
            statement.setString(4, w[3]);
            statement.setString(5, w[4]);
            statement.addBatch();
            row++;
         
            
            if ( row % batchInterval == 0) 
            	statement.executeBatch();
            
            line = r1.readLine();
		 }
		statement.executeBatch();
		long end = System.currentTimeMillis();
		
		if(row>=1000000)
		{	
			System.out.println(row + " Rows Update into Database");
			System.out.println((end - start)/1000f + " Seconds" +" OR "+ (end - start)+ " MilliSeconds");
		}
		else
		{   statement.executeUpdate("TRUNCATE table Demo");
			throw new Exception(" FILE Record is Less than 1000000.....Please Check the file once Before uploading into database ");  
		}
		
		statement.close();
		connection.close();	
	}
}
