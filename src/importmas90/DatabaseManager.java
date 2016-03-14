/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package importmas90;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
class DatabaseManager {

    private final String JDBC_CONNECTION_URL;
    private final String DB_URL;
    private final String USER;
    private final String PASS;
    private final String JDBC_CONNECTION_URL1;
    private final String DB_URL1;
    private final String USER1;
    private final String PASS1;
    private final Properties prop;
    
    public DatabaseManager(String JDBC_CONNECTION_URL, String DB_URL, String USER, String PASS, String JDBC_CONNECTION_URL1, String DB_URL1, String USER1, String PASS1, Properties prop) {
        this.JDBC_CONNECTION_URL = JDBC_CONNECTION_URL;
        this.DB_URL = DB_URL;
        this.USER = USER;
        this.PASS = PASS;
        this.JDBC_CONNECTION_URL1 = JDBC_CONNECTION_URL1;
        this.DB_URL1 = DB_URL1;
        this.USER1 = USER1;
        this.PASS1 = PASS1;
        this.prop = prop;
    }
    private Connection getConnection()
    {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL, USER,PASS);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }

    private Connection getConnection2() {
        Connection connection = null;
        
        try 
        {
            
            Class.forName(JDBC_CONNECTION_URL1);

        } catch (ClassNotFoundException e) 
        {

            System.out.println(e.getMessage());

        }
        try 
        {

            connection = DriverManager.getConnection(DB_URL1, USER1,PASS1);
            return connection;

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        }
        
        return connection;
    }
    
    public void callStoredProcedure(String Storedproc, String ProcedureName) 
    {
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        Connection dbConnection = getConnection();
	CallableStatement callableStatement = null;
        try {
            callableStatement = dbConnection.prepareCall(Storedproc);
            callableStatement.executeUpdate();
            successful = 1;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            successful = 0;
        } finally 
        {
 
            if (callableStatement != null) {
                try {
                    callableStatement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
        
    }
    
    public void loadCSV(String ProcedureName, File filePath, String table, String col_Names, int id, boolean header) {
        
        PreparedStatement preparedStatement = null;
        Connection dbConnection = null;
        CurrentTime ct = new CurrentTime(prop);
        String currentTime = ct.getCurrentTime();
        int successful = 0;

        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";
        
        String questionmarks = null;
        String SQL_INSERT = "INSERT INTO ${table}(${keys}) VALUES(${values})";
        String TABLE_REGEX = "\\$\\{table\\}";
        String KEYS_REGEX = "\\$\\{keys\\}";
        String VALUES_REGEX = "\\$\\{values\\}";
            
        try {
            
        
            br = new BufferedReader(new FileReader(filePath));
            String[] headerRow;
                
            if (header == true) {
                String headerRowString = br.readLine();
                headerRow = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            } else {
                headerRow = col_Names.split(",");
            }

            if (null == headerRow) {
                    throw new FileNotFoundException(
                                    "No columns defined in given CSV file." +
                                    "Please check the CSV file format.");
            }
            
            for(int i = 0; i < headerRow.length; i++)
            {
                if(i == 0)
                {
                    questionmarks = "?,";
                } else if(i < headerRow.length - 1)
                {
                    questionmarks = questionmarks + "?,";
                } else
                {
                    questionmarks = questionmarks + "?";
                }
                  
            }

            String query = SQL_INSERT.replaceFirst(TABLE_REGEX, table);
            query = query.replaceFirst(KEYS_REGEX, col_Names);
            query = query.replaceFirst(VALUES_REGEX, questionmarks);
            
            dbConnection = getConnection();
            dbConnection.setAutoCommit(false);
            preparedStatement = dbConnection.prepareStatement(query);
            
            if(id == 1) 
            {
                dbConnection.createStatement().execute("DELETE FROM " + table);	
            }
            
            while((line = br.readLine()) != null)
            {

                String headerRowString = line;
                String[] rowCount = headerRowString.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
                
                int index = 0;
                for(int i = 1; i <= rowCount.length; i++)
                {
                    String insert = rowCount[index];
                    
                    if(insert.contains("\""))
                    {
                       preparedStatement.setString(i, insert.subSequence(1, insert.lastIndexOf("\"")).toString());
                       index++;
                    }else
                    {
                       preparedStatement.setString(i, insert);
                       index++;
                    }
                    
                }
                
                preparedStatement.addBatch();
            }
            
            preparedStatement.executeBatch();
            dbConnection.commit();
            successful = 1;
            
        } catch (FileNotFoundException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } catch (IOException | SQLException ex) {
            System.out.println("LoadCSV Error: " + ex);
            successful = 0;
        } finally 
        {
            try {
                if (br != null)
                    br.close();
                if (null != preparedStatement)
                    preparedStatement.close();
                if (null != dbConnection)
                    dbConnection.close();
            } catch (IOException | SQLException ex) {
                System.out.println("LoadCSV Failed " + ex);
                successful = 0;
            }
            
        }
        
        updateStatusTable(ProcedureName, currentTime, successful);
    }
    
    private void updateStatusTable(String ProcedureName, String currentTime, int successful)
    {

        Connection dbConnection = getConnection2();
        Statement statement = null;
        String updateTable = "UPDATE mgex_riskmgnt_scheduled_tasks " + 
                "SET Last_Run_Time = '" + currentTime + "', " + 
                "Success = " + successful + " WHERE Procedures = '" + ProcedureName + "';";
        
        try 
        {
            statement = dbConnection.createStatement();

            statement.execute(updateTable);

        } catch (SQLException e) 
        {

            System.out.println(e.getMessage());

        } finally 
        {

            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

            if (dbConnection != null) {
                try {
                    dbConnection.close();
                } catch (SQLException ex) {
                    System.out.println(ex.getMessage());
                }
            }

        }
    }
}
