/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package database;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Georgiy
 */
public class threads {
    public Query queryChess;
    
    /* connecting with DataBase */
    public boolean connectDataBase(){
        return queryChess.connectDataBase();
    }
    
    /* disconnect deleted all Database object */
    public boolean disconnect(){
        return queryChess.disconnect();
    }
    
    /* show selected table */
    public boolean showTable() throws InterruptedException{
        return queryChess.showTable();
    }
   
    /* add new row */
    public void addRows(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                queryChess.addRows();
            }
        }).start();
    }
    
    /* This method removes the selected row */
    public void deletedRow(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queryChess.deletedRow();
                } catch (InterruptedException ex) {
                    Logger.getLogger(threads.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    };
    
    /* Update data due to mouse click / mouse scroll / presse key */
    public void updateRow(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queryChess.updateRow();
                } catch (InterruptedException ex) {
                    Logger.getLogger(threads.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    }
    
    /* add new rows in table */
    public void reloadRows(int position, int mode){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queryChess.reloadRows(position,mode);
                } catch (InterruptedException ex) {
                    Logger.getLogger(threads.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    }
    
    /* push searching request */
    public void okGoogle(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    queryChess.okGoogle();
                } catch (InterruptedException ex) {
                    Logger.getLogger(threads.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        }).start();
    }
    
    /* show depend table */
    public void showSlaveTable(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                queryChess.showSlaveTable();
            }
        }).start();
    }
}
