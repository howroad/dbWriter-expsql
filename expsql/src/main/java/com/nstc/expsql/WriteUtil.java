package com.nstc.expsql;


import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 * Title: WriteTest.java
 * </p>
 *
 * @author luhao
 * 
 * @since：2018年12月21日 下午1:24:52
 * 
 */
public class WriteUtil {
    
    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }


    
    public static void buildDate(Table table,String sql,String[] primaryColUpKeys,String filName) {
        PrintWriter out = null;
        try {
            File dir = new File(TableContans.PATH + table.getTableName() + "\\fromDB\\");
            dir.mkdirs();
            out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(filName, false), "GBK"));
            List<List<Object>> dataList = getDataBySQL(sql);
            table.writeDate(out, dataList, primaryColUpKeys);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            out.close();
        }
    }
    
    
    public static List<List<Object>> getDataBySQL(String sql) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<List<Object>> resultList = new ArrayList<List<Object>>();
        try {
            conn = DriverManager.getConnection(TableContans.URL, "TA0723DEV", "123456");
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            int count = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                List<Object> list = new ArrayList<Object>();
                for (int i = 0; i < count; i++) {
                    list.add(rs.getObject(i + 1));
                }
                resultList.add(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return resultList;
    }

}
