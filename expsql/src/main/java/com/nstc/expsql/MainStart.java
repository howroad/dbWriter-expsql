package com.nstc.expsql;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.CaseFormat;

import oracle.jdbc.driver.OracleConnection;

/**
 * <p>Title: MainStart.java</p>
 *
 * <p>Description: </p>
 *
 * <p>Company: 北京九恒星科技股份有限公司</p>
 *
 * @author luhao
 * 
 * @since：2019年4月29日 上午9:32:34
 * 
 */
public class MainStart {
    public static void main(String[] args) {
        buildTA0723SQL();
    }
    
    
    static void buildTA0723SQL() {
        String buss_idStr = "0,1,2,3,4";
        String[] tableNames = new String[] {
                "GDT_CUST_BUSS","GDT_CUST_BUSS_EXTRA",
                "GDT_CUST_FIXED_ELEMENT","GDT_CUST_ELEMENT",
                "GDT_CUST_TYPE","GDT_CUST_TEMPLATE",
                "GDT_CUST_EXTRA",
                "UM_CODE"};
        
        String[] sqls = new String[] {
                "SELECT * FROM GDT_CUST_BUSS T WHERE T.BUSS_ID IN (" + buss_idStr + ") ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_BUSS_EXTRA T WHERE T.BUSS_ID IN (" + buss_idStr + ") ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_FIXED_ELEMENT T ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_ELEMENT T WHERE T.BUSS_ID IN (" + buss_idStr + ") ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_TYPE T ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_TEMPLATE ORDER BY 1 ASC",
                "SELECT * FROM GDT_CUST_EXTRA ORDER BY 1 ASC",
                "SELECT u.* FROM UM_CODE U INNER JOIN um_ctype t on u.ctid=t.ctid where t.ctype in ('CLMS01','CLMS02','CLMS03','CLMS04')"};
        
        List<String[]> primaryKeys = new ArrayList<String[]>();
        primaryKeys.add(new String[] {"BUSS_ID"});
        primaryKeys.add(new String[] {"BE_ID"});
        primaryKeys.add(new String[] {"FIXED_ID"});
        primaryKeys.add(new String[] {"ELEMENT_ID"});
        primaryKeys.add(new String[] {"TYPE_NAME","TYPE_TYPE"});
        primaryKeys.add(new String[] {"TEMPLATE_ID"});
        primaryKeys.add(new String[] {"EXTRA_ID"});
        primaryKeys.add(new String[] {"MCODE"});
        
        for (int i = 0; i < tableNames.length; i++) {
            String tableName = tableNames[i];
            String sql = sqls[i];
            String[] primaryKey = primaryKeys.get(i);
            Table table = buildTableFromDB(tableName);
            String filName = TableContans.PATH + "\\sqls\\" + table.getTableName() + ".SQL";
            File file = new File(filName);
            file.getParentFile().mkdirs();
            WriteUtil.buildDate(table,sql,primaryKey,filName);
            System.out.println("build " + tableName + ".SQL down...");
        }
        
    }
    
    public static Table buildTableFromDB(String tableName) {
        tableName = tableName.toUpperCase();
        Connection conn = null;
        DatabaseMetaData db = null;
        // 字段信息
        ResultSet rs = null;
        // 表信息
        ResultSet rsTable = null;
        String tableRemark = null;
        List<MyParam> paramList = new ArrayList<MyParam>();
        try {
            conn = DriverManager.getConnection(TableContans.URL,
                    TableContans.USER, TableContans.PWD);
            ((OracleConnection) conn).setRemarksReporting(true);
            db = conn.getMetaData();
            rs = db.getColumns(null, TableContans.USER, tableName.toUpperCase(), null);
            rsTable = db.getTables(null, TableContans.USER, tableName.toUpperCase(), new String[] {"TABLE"});
            if(rsTable.next()) {
                tableRemark = rsTable.getString("REMARKS");
            }
            while (rs.next()) {
                // 列名
                String columnName = rs.getString("COLUMN_NAME");
                // 字段名称
                String paramName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                // 数据类型
                int columnType = rs.getInt("DATA_TYPE");
                // 小数位数
                int decimalDigits = rs.getInt("DECIMAL_DIGITS");
                // 注释
                String remark = rs.getString("REMARKS");
                //精度
                int columnSize = rs.getInt("COLUMN_SIZE");
                
                MyParam param = new MyParam(paramName, columnName, remark, columnType, columnSize, decimalDigits);
                paramList.add(param);
            }
        } catch (Exception e) {
            e.getStackTrace();
            throw new RuntimeException(e);
        }finally {
            try {
                if(rs!=null) {
                    rs.close();
                }
                if(rsTable != null) {
                    rsTable.close();
                }
                if(conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if(paramList == null || paramList.isEmpty()) {
            throw new RuntimeException(tableName + "表中无字段");
        }
        Table table = new Table(tableName, tableRemark ,paramList);
        return table;
    }
}
