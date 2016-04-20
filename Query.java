/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;
import java.awt.Dimension;
import static java.lang.Thread.sleep;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.table.DefaultTableModel;
/**
 *
 * @author Georgiy
 */
public class Query implements Runnable {
    
    public JTable jTable1;
    public int count_header = 0;
    public Vector <String> headerVect, old_data, new_data, external_keys_vec;
    public JComboBox jComboBox1;
   
    private static Semaphore mutex = new Semaphore(1);
    private static Connection connect;
    private static Statement stmt;
    private static DatabaseMetaData dbmd;
    private DefaultTableModel model;
    private int indexSelected;
    private int old_string = 0, new_string = 0;
    private int countTables = 0;
    private int Limit = 5000;
    private int start_lim = 0;
    private String nameDB, nameUser, pass, table, keyWord;
    private String header_slave_table;
    private boolean addNewRow = false;


    public void run(){};
    
    /* connecting with DataBase */
    public boolean connectDataBase(){
        try {
            // opening database connectnection to MySQL server
            connect = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+nameDB,nameUser, pass);
            stmt = connect.createStatement();
            dbmd = connect.getMetaData();
            
            // get list of the tables
            ResultSet rs2 = dbmd.getTables(null, null, null, null);
            while (rs2.next()) {
                jComboBox1.addItem(rs2.getString(3));
                countTables++;
            }
            
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 004: "+ex);
        }
        
        return false;
    }
    
    /* disconnect deleted all Database object */
    public boolean disconnect(){
        try {
            if (stmt != null)                                                 // check obkect BD
                 stmt.close();
            
            for(int i = 1; i<countTables;i++)                                   // delete list tables
                jComboBox1.removeItemAt(1);
            
            countTables = 1;                                                    // zeroing counter
            headerVect = new Vector<>();
            initHeaderTable();
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 002: "+ex);
        }
        return false;
    }
    
    /* show selected table */
    public boolean showTable() throws InterruptedException{


        try {
            ResultSet header_table = dbmd.getColumns(null, null, table, null);  // get header columns
            mutex.acquire();
            headerVect = new Vector<>();
            count_header = 0;
            old_string=0;
            while (header_table.next()) {
                headerVect.add(header_table.getString(4));                      // save header
                count_header++;                                                 // header count
            }
            initHeaderTable();
            
            String query = createdShow();
            ResultSet content = stmt.executeQuery(query);
            
            while (content.next()) {                                                 // get data
                Vector<String> newRow = new Vector<>();
                for(int i = 1; i< count_header+1; i++){
                    newRow.add(content.getString(i));                                // create string
                }
                model.addRow(newRow);                                             // add string
            }
            
            // start init for table 
            jTable1 .getSelectionModel().setSelectionInterval(0, 0);            // choose fisrt string
            old_data = new Vector();
            for(int i=0;i<count_header;i++){                                    // save context for first string
               old_data.add((String) jTable1.getValueAt(old_string,i)); 
            }
            mutex.release();
            start_lim = Limit;
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 003: "+ex);
            mutex.release();
        }
        return false;
    }
   
    /* add new row */
    public void addRows(){
        try {
            Vector<String> newRow = new Vector<>();
            mutex.acquire();
            model.addRow(newRow);
            jTable1.setModel(model);
            for(int i=0;i<count_header;i++){
                jTable1.setValueAt("null", jTable1.getRowCount()-1, i);
            }
            mutex.release();
            start_lim = Limit;
            addNewRow = true;
        } catch (InterruptedException ex) {
            mutex.release();
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    /* This method removes the selected row */
    public void deletedRow() throws InterruptedException{
        try {
            setSelectedRow();
            String query = createdDeletedQuery();
            stmt.execute(query);
            mutex.acquire();
            model.removeRow(indexSelected);
            mutex.release();
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 007: "+ex);
            mutex.release();
        }
    };
    
    /* Update data due to mouse click / mouse scroll / presse key */
    public boolean updateRow() throws InterruptedException{
        try {    
                mutex.acquire();
                new_string = indexSelected;
                String query = "";                                              
                boolean go_request = false;
                
                new_data = new Vector();                                        
                for(int i=0;i<count_header;i++){
                   new_data.add((String) jTable1.getValueAt(old_string,i));     // save updates data of the old row
                }
                
                for(int i = 0; i<count_header;i++ ){                            // check update old row data
                    if(!new_data.get(i).equals(old_data.get(i))){               
                        go_request=true;                                        // if data changed - set flag
                        break;
                    }
                }
                if(go_request){                                                 // if data changed
                        if(addNewRow && old_string == jTable1.getRowCount()-1 ){ // add new row in table
                            query = createdInsert();                            
                            stmt.execute(query);
                            addNewRow = false;                               // reset flag-add    
                        }else{                                                  // update row
                            query = createdUpdate();                                     
                            stmt.executeUpdate(query);
                        }
                }
                old_string = new_string;                                        // old row = new row
                old_data = new Vector();
                for(int i=0;i<count_header;i++){
                   old_data.add((String) jTable1.getValueAt(old_string,i)); 
                }
                mutex.release();
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 001: "+ex);
            mutex.release();
        }
        return false;
    }
    
    /* add new rows in table */
    public boolean reloadRows(int position, int mode) throws InterruptedException{
        try {
            int max_position = jTable1.getHeight() - 403;                       // minus border
            if(max_position == position){
                String query ="";
                switch(mode){
                    case(1):
                        query= createdShow();
                        break;
                    case(2):
                        query= createdGoogleRequest();
                        break;
                }
                ResultSet content = stmt.executeQuery(query);
                mutex.acquire();
                while (content.next()) {                                    // get data
                    Vector<String> newRow = new Vector<>();
                    for(int i = 1; i< count_header+1; i++){
                        newRow.add(content.getString(i));                   // create string
                    }
                    model.addRow(newRow);                                     // add string
                }

                // start init for table 
                jTable1 .getSelectionModel().setSelectionInterval(0, 0);    // select fisrt row
                old_data = new Vector();
                for(int i=0;i<count_header;i++){                            // save context for first row
                   old_data.add((String) jTable1.getValueAt(old_string,i)); 
                }
                
                start_lim += Limit;
            }
            mutex.release();
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 009: "+ex);
            mutex.release();
        }
        return false;
    }
    
    /* push searching request */
    public boolean okGoogle() throws InterruptedException{
        String query, headerLine;
        try {
            mutex.acquire();
            old_string = 0;
            headerLine = getMasterHeader();                                         // get headerlines of the select table
            if(!headerLine.equals(""))
                header_slave_table = headerLine;
            else
                return false;
            
            headerVect.add("KEYTABLE");                                         // add special columns 
            headerVect.add("KEYWORD");
            
            initHeaderTable();                                                  // init header select table                                  
            hideSpecialColumns();                                               // hide special colunms

            query = createdExternalKey();                                       // get external_keys
            ResultSet external_keys = stmt.executeQuery(query);
            
            external_keys_vec = new Vector<>();
             while (external_keys.next()) {
                external_keys_vec.add(external_keys.getString(1));              // master_col
                external_keys_vec.add(external_keys.getString(2));              // name_slave
                external_keys_vec.add(external_keys.getString(3));              // slave_col
             }
             
            query = createdGoogleRequest();                                     // return complex compound request
            ResultSet slave = stmt.executeQuery(query);
            while (slave.next()) {                                              // get data
                Vector<String> newRow = new Vector<>();
                for(int j = 1; j< count_header+3; j++){
                    newRow.add(slave.getString(j));                             // create string
                }
                model.addRow(newRow);                                             // add string
            }

            // initialization table
            jTable1 .getSelectionModel().setSelectionInterval(0, 0);            // choose fisrt string
            old_data = new Vector();
            for(int i=0;i<count_header;i++){                                    // save context for first string
               old_data.add((String) jTable1.getValueAt(old_string,i)); 
            }
            mutex.release();
            return true;
        } catch (SQLException ex) {
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Error 010: "+ex);
            mutex.release();
        }
        return false;
    }
    
    /* show depend table */
    public boolean showSlaveTable(){
        if(!jTable1.getValueAt(new_string,count_header).equals("KEYTABLE")){
            JFrame dependent_window = new JFrame("dependent_window");
            try {
                // give headerline of the dependent table
                ResultSet rs4 = dbmd.getColumns(null, null, (String) jTable1.getValueAt(new_string,count_header), null);
                int d_header = 0;
                Vector <String> header = new Vector();
                Vector <String> lines = new Vector();
                while (rs4.next()) {
                    header.add(rs4.getString(4));                                // save header
                    d_header++;                                                  // header count
                }
                String query = "select * from " + (String) jTable1.getValueAt(new_string,count_header) 
                        + " where "+(String) jTable1.getValueAt(new_string,count_header+1)+";";
                ResultSet rs = stmt.executeQuery(query);

                while (rs.next()) {                                             // get data
                    for(int i = 1; i< d_header+1; i++){
                        lines.add(rs.getString(i));                             // create string
                    }                                                           // add string
                }
                JTable jTable2 = new JTable(0,0);
                DefaultTableModel model2 = new DefaultTableModel(header, 0);      // set header
                jTable2.setModel(model2);                                         // update table
                model2.addRow(lines);                                             // add string
                //открываем вторую
                JScrollPane scrollPane = new JScrollPane(jTable2);
                dependent_window.getContentPane().add(scrollPane);
                dependent_window.setPreferredSize(new Dimension(860,150));
                dependent_window.pack();
                dependent_window.setLocationRelativeTo(null);
                dependent_window.setVisible(true);
                jTable2.setEnabled(false);
                return true;
            } catch (SQLException ex) {
                Logger.getLogger(main_interface.class.getName()).log(Level.SEVERE, null, ex);
            }
       }else{
            JFrame frame = new JFrame("JOptionPane showMessageDialog example");
            JOptionPane.showMessageDialog(frame,"Keyword in this line");
       }
        
        return false;
    }
    

    /* Return headerLine of the table */
    private String getMasterHeader(){
        String headerLine = "";
        headerVect = new Vector<>();
        try {
            ResultSet header_main = dbmd.getColumns(null, null, table, null);   // get the headerlines of the main table
            count_header = 0;
            while (header_main.next()) {
                headerVect.add(header_main.getString(4));
                headerLine = headerLine + header_main.getString(4);
                count_header++;
                if(header_main.next())                                          // if not last element, add ","
                    headerLine = headerLine+",";
                header_main.previous();
            }
            return headerLine;
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }
    
    /* Hide columns of the support */
    private void hideSpecialColumns(){
        jTable1.getColumnModel().getColumn(count_header).setMinWidth(0);
        jTable1.getColumnModel().getColumn(count_header).setMaxWidth(0);
        jTable1.getColumnModel().getColumn(count_header).setPreferredWidth(0);
        jTable1.getColumnModel().getColumn(count_header).setResizable(false);
        jTable1.getColumnModel().getColumn(count_header+1).setMinWidth(0);
        jTable1.getColumnModel().getColumn(count_header+1).setMaxWidth(0);
        jTable1.getColumnModel().getColumn(count_header+1).setPreferredWidth(0);
        jTable1.getColumnModel().getColumn(count_header+1).setResizable(false);
    }
    
    /* Its create a string for the request */
    private String createdDeletedQuery(){
        String query = "DELETE FROM `" + table + "` WHERE ";
        for(int i =0; i< count_header; i++){
            query = query + (String)headerVect.get(i)+" = '"+(String) jTable1.getValueAt(indexSelected,i)+"'";
            if(i+1<count_header)
                query = query +" and ";
        }   
        return query;
    };
    
    /* Method for create string show-qeury */
    private String createdShow(){
        return  "select * from " + table + " LIMIT " + start_lim + ","+Limit+";";
    }
    
    /* Create inset-request */
    private String createdInsert(){
        String query = "INSERT INTO `" + table + "` ( ", name;
        for(int i =0; i< count_header; i++){
            query = query + (String)headerVect.get(i);
            if(i+1<count_header)
                query = query +", ";
        }
        query = query +") VALUES ( ";
        for(int i =0; i< count_header; i++){
            query = query + "'"+(String)new_data.get(i)+"'";
            if(i+1<count_header)
                query = query +" , ";
        }
        return query +");";
    }
    
    /* Create update-request */
    private String createdUpdate(){
        // Generate request
        String query = "update `" + table + "` set ";
        for(int i =0; i< count_header; i++){
            query = query + (String)headerVect.get(i)+" = '"+(String)new_data.get(i)+"'";
            if(i+1<count_header)
                 query = query +", ";
        }
        query = query +" where ";
        for(int i =0; i< count_header; i++){
            query = query + (String)headerVect.get(i)+" = '"+(String)old_data.get(i)+"'";
            if(i+1<count_header)
                 query = query +" and ";
        }
        return query +";";
    }
    
    /* Create request for get external keys */
    private String createdExternalKey(){
        String query = "SELECT COLUMN_NAME as master_col, REFERENCED_TABLE_NAME as "
                + "name_slave, REFERENCED_COLUMN_NAME as slave_col FROM "
                + "information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA ='" + nameDB + "'"
                + " AND TABLE_NAME ='" + table + "' AND CONSTRAINT_NAME <>'PRIMARY' "
                + "AND REFERENCED_TABLE_NAME is not null;";
        return query;
    }
  
    /* Return complex compound query */
    private String createdGoogleRequest(){
        try{
            String query = "select *, 'KEYTABLE', 'KEYWORD' from " + table + " where concat("
                        + header_slave_table + ") like '%" + keyWord + "%' LIMIT " + start_lim + "," + Limit + " ";
            String header_slave_table = "";
            for(int i = 0 ; i<external_keys_vec.size();i++){
                    header_slave_table = "";
                    // get header columns
                    ResultSet header = dbmd.getColumns(null, null, external_keys_vec.get(i+1), null);
                    while (header.next()) {
                        header_slave_table = header_slave_table + "T2."+header.getString(4);
                        if(header.next())    // if not last element, add ","
                            header_slave_table = header_slave_table+",";
                        header.previous();
                    }
                    
                    // Keyword searching in neighboring tables
                    query = query +  " UNION select T1.*,'"+external_keys_vec.get(i+1)+ "', "
                            + "concat_ws('=','" + external_keys_vec.get(i+2) + "',T2." + external_keys_vec.get(i+2) +
                            ") from " + table + " as T1\n" +
                            "inner join " + external_keys_vec.get(i+1) + " as T2 on concat"
                            + "(" + header_slave_table + ") like '%"+keyWord+"%'\n" +
                            "where T1." + external_keys_vec.get(i)+ " = T2." + external_keys_vec.get(i+2) + 
                            " LIMIT " + start_lim + "," + Limit;
                    i=i+2;
            }  
            
            return query + ";";
            
        } catch (SQLException ex) {
            Logger.getLogger(Query.class.getName()).log(Level.SEVERE, null, ex);
        }
        return "";
    }
    
    /* Reinit header table */
    private void initHeaderTable(){
        model = new DefaultTableModel(headerVect, 0);                             
        jTable1.setModel(model);
    }
    
    
    
    /* Set the index of the selected row */
    public void setSelectedRow(){
        indexSelected = jTable1.getSelectedRow();
    }

    /* Set the database name */
    public void setNameDB(String name){
        nameDB = name;
    }

    /* Set a user name */
    public void setUser(String user){
        nameUser = user;
    }

    /* Set password */
    public void setPass(String password){
        pass = password;
    }
    
    /* Set name of the table */
    public void setNameTable(String nameTable){
        table = nameTable;
    }
    
    /* Set word which to be searched for in BD */
    public void setGoogleWord(String word){
        keyWord = word;
    }
    
    /* Reset value */
    public void zeroingStartLimit(){
        start_lim = 0;
    }
}
