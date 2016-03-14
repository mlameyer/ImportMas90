/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package importmas90;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 *
 * @author mlameyer <mlameyer@mgex.com>
 */
public class ImportMas90 {
    private static CurrentTime ct;
    private final static String ProcedureName = "ImportMas90";
    private final static String Storedproc = "CALL sp_parseMAS90()";
    private static DatabaseManager dbm;
    private static String FullPathandName;
    private static String FilePath, FileName;
    private static final String table = "t_datafinance_mas90tb_wrk";
    private static final String col_Names = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z,AA,AB,AC,AD,AE";
    private static final int id = 1;
    private static final boolean header = true;
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        InputStream inputStream = null;
        Properties prop = new Properties();
        try {
            String propFileName = "C:\\Program Files\\AutomationControlapp\\ImportMas90\\config.properties";
            inputStream = new FileInputStream(propFileName);
            
            prop.load(inputStream);
            
            String JDBC_CONNECTION_URL = prop.getProperty("local_JDBC_CONNECTION_URL");
            String DB_URL = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
            String USER = prop.getProperty("local_DB_USER");
            String PASS = prop.getProperty("local_DB_PASS");
            String JDBC_CONNECTION_URL1 = prop.getProperty("local_JDBC_CONNECTION_URL");
            String DB_URL1 = prop.getProperty("local_DB_URL") + prop.getProperty("local_DB_Risk");
            String USER1 = prop.getProperty("local_DB_USER");
            String PASS1 = prop.getProperty("local_DB_PASS");
        
            FilePath = prop.getProperty("Mas90FilePath");
            FileName = prop.getProperty("Mas90FileName");
            
            ct = new CurrentTime(prop);
            dbm = new DatabaseManager(JDBC_CONNECTION_URL, DB_URL, USER, PASS, JDBC_CONNECTION_URL1, DB_URL1, USER1, PASS1, prop);
            
            FullPathandName = FilePath + FileName.substring(0, FileName.indexOf(".")) + ct.getCurrentDate() + FileName.substring(FileName.indexOf(".")); 
            File filePath = new File(FullPathandName);

            dbm.loadCSV(ProcedureName, filePath, table, col_Names, id, header);
            dbm.callStoredProcedure(Storedproc, ProcedureName);
            
        } catch (FileNotFoundException ex) {
            
        } catch (IOException ex) {

        } finally {
            try {
                inputStream.close();
            } catch (IOException ex) {

            }
        }
    }
 
}
