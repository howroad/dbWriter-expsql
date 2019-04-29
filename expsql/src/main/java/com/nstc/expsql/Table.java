package com.nstc.expsql;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.CaseFormat;

import oracle.sql.TIMESTAMP;

/**
 * 
 * <p>
 * Title: Table.java
 * </p>
 *
 * <p>
 * Description:
 * </p>
 *
 * @author luhao
 * 
 * @since：2018年12月26日 下午4:56:49
 *
 */
public class Table {

    private String tableName;
    private String tableRemart;
    private Map<String, String> map = new HashMap<String, String>();
    private List<MyParam> paramList;

    public Table(String tableName, String tableRemark, List<MyParam> paramList) {
        super();
        this.tableName = tableName.toUpperCase();
        this.tableRemart = tableRemark == null || "null".equals(tableRemark) ? "" : tableRemark;
        this.paramList = paramList;

    }

    private void initMap() {
        if (hasDateType()) {
            map.put("import", "import java.util.Date;");
        }
        map.put("entityName", getEntityName());
        map.put("entityNameLow", getEntityName().toLowerCase());
        map.put("remark", this.tableRemart);
        map.put("now", getNow());
        map.put("tableName", getTableName());
        map.put("pkName", paramList.get(0).getParamName());
        map.put("pkColumnName", paramList.get(0).getColumnName());
        map.put("pkNameUp", CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, paramList.get(0).getParamName()));
    }

    /**
     * 是否有时间类型
     * 
     * @Description:
     * @return boolean
     * @author luhao
     * @since：2019年1月30日 下午4:44:48
     */
    private boolean hasDateType() {
        if (paramList == null || paramList.isEmpty()) {
            throw new RuntimeException("无列表信息！");
        }
        for (MyParam myParam : paramList) {
            if (myParam == null) {
                throw new RuntimeException("Param中无信息！");
            }
            if (myParam.getType().getValue() == Types.DATE || myParam.getType().getValue() == Types.TIMESTAMP) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获得插入语句的字段，例如to_date('','')
     * 
     * @param obj
     * @param line
     * @return String
     * @author luhao
     * @since：2018年12月26日 下午5:17:29
     */
    private String getInsertValue(Object obj, MyParam param) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        int columnType = param.getType().getValue();
        String result = null;
        if (obj == null) {
            result = "null";
        } else if (Types.DECIMAL == columnType) {
            result = String.valueOf(obj);
        } else if (Types.VARCHAR == columnType || Types.CHAR == columnType) {
            String str = String.valueOf(obj);
            str = str.replaceAll("'", "''");
            result = "'" + str + "'";
        } else if (Types.TIMESTAMP == columnType || Types.DATE == columnType) {
            if (obj.getClass() == oracle.sql.TIMESTAMP.class) {
                TIMESTAMP time = (TIMESTAMP) obj;
                Date dateTime = null;
                try {
                    dateTime = time.dateValue();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                String value = sdf.format(dateTime);
                result = "TO_DATE('" + value + "', 'YYYY-MM-DD HH24:MI:SS')";
            } else {
                String value = sdf.format(obj);
                result = "TO_DATE('" + value + "', 'YYYY-MM-DD HH24:MI:SS')";
            }
        } else {
            throw new RuntimeException("未知类型！");
        }
        return result;
    }

    /**
     * 根据集合集合内容生成数据库插入语句
     * 
     * @param out
     * @param dataList
     * @return void
     * @author luhao
     * @since：2018年12月28日 下午6:23:39
     */
    public void writeDate(PrintWriter out, List<List<Object>> dataList, String[] primaryColUpKeys) {
        String[] primaryKeyValues = new String[primaryColUpKeys.length];
        for (List<Object> list : dataList) {
            out.print("INSERT INTO " + tableName + "(");
            for (ListIterator<MyParam> iterator = paramList.listIterator(); iterator.hasNext();) {
                MyParam param = iterator.next();
                String columnName = param.getColumnName();
                if (iterator.hasNext()) {
                    out.print(columnName + ",");
                } else {
                    out.println(columnName + ") ");
                }
            }
            out.print("SELECT ");
            int index = 0;
            for (ListIterator<Object> iterator = list.listIterator(); iterator.hasNext();) {
                boolean last = false;
                Object object = iterator.next();
                MyParam param = paramList.get(index++);
                String columnName = param.getColumnName();
                String value = getInsertValue(object, param);
                for (int i = 0; i < primaryKeyValues.length; i++) {
                    if (columnName.equals(primaryColUpKeys[i])) {
                        primaryKeyValues[i] = value;
                    }
                }
                last = !iterator.hasNext();
                if (last) {
                    out.print(value);
                } else {
                    out.print(value + ",");
                }
            }
            StringBuffer whereStr = new StringBuffer(128);
            for (int i = 0; i < primaryColUpKeys.length; i++) {
                String primaryColUpKey = primaryColUpKeys[i];
                String primaryKeyValue = primaryKeyValues[i];
                if (i > 0) {
                    whereStr.append(" AND ");
                }
                whereStr.append(primaryColUpKey + " = " + primaryKeyValue);
            }
            out.println(" FROM DUAL ");
            out.println("WHERE NOT EXISTS (SELECT 1 FROM " + tableName + " WHERE " + whereStr + ")");
            out.println("/");
        }

    }

    /**
     * 获得字符串格式的当前的时间
     * 
     * @Description:
     * @return String
     * @author luhao
     * @since：2018年12月28日 下午6:30:40
     */
    private String getNow() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date());
    }

    /**
     * 获得实体类的名称
     * 
     * @Description:
     * @return
     * @return String
     * @author luhao
     * @since：2018年12月28日 下午6:32:04
     */
    public String getEntityName() {
        String entityName = null;
        if (tableName.contains(TableContans.UNDER_LINE)) {
            entityName = tableName.substring(tableName.indexOf(TableContans.UNDER_LINE));
        } else {
            entityName = tableName;
        }
        return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, entityName);
    }

    /**
     * 获得JavaBean的名称
     * 
     * @Description:
     * @return
     * @return String
     * @author luhao
     * @since：2018年12月28日 下午6:31:36
     */
    public String getJaveBeanFileName() {
        return getEntityName() + TableContans.PO + ".java";
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableRemart() {
        return tableRemart;
    }

    public void setTableRemart(String tableRemart) {
        this.tableRemart = tableRemart;
    }

    @Override
    public Table clone() {
        return new Table(tableName, tableRemart, paramList);
    }

    @Override
    public String toString() {
        return "Table [tableName=" + tableName + ", tableRemart=" + tableRemart + "]";
    }

    public Map<String, String> getMap() {
        initMap();
        return map;
    }

    public List<MyParam> getParamList() {
        return paramList;
    }

    public void setParamList(List<MyParam> paramList) {
        this.paramList = paramList;
    }

}

class MyParam {

    private String paramName;
    private String paramRemark;
    private String columnName;
    private MyType type;
    private Map<String, String> map = new HashMap<String, String>();

    public MyParam(String paramName, String columnName, String paramRemark, int dateType, int columnSize, int decimalDigits) {
        super();
        this.type = new MyType(dateType, columnSize, decimalDigits);
        this.paramName = paramName;
        this.paramRemark = paramRemark;
        this.columnName = columnName;
        init();
    }

    public MyParam(String paramRemark, String columnName, String typeStr) {
        super();
        this.paramRemark = paramRemark;
        this.columnName = columnName.toUpperCase();
        this.paramName = CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
        String columnType = typeStr.trim().toUpperCase();
        Matcher mat = Pattern.compile("(?<=\\()(\\S+)(?=\\))").matcher(typeStr);
        if (TableContans.DATE.equals(columnType)) {
            this.type = new MyType(Types.DATE, 0, 0);
        } else if (TableContans.TIMESTAMP.equals(columnType)) {
            this.type = new MyType(Types.TIMESTAMP, 0, 0);
        } else if (columnType.startsWith(TableContans.NUMBER)) {
            if (mat.find()) {
                String inner = mat.group();
                this.type = new MyType(Types.DECIMAL, new Integer(inner.split(",")[0]), new Integer(inner.split(",")[1]));
            } else {
                this.type = new MyType(Types.DECIMAL, 0, 0);
            }
        } else if (columnType.startsWith(TableContans.VARCHAR2)) {
            if (mat.find()) {
                this.type = new MyType(Types.VARCHAR, new Integer(mat.group()), 0);
            } else {
                throw new RuntimeException("类型错误");
            }
        } else if (columnType.startsWith(TableContans.INTEGER)) {
            this.type = new MyType(Types.DECIMAL, 0, 0);
        } else {
            throw new RuntimeException("未知类型 : " + columnType);
        }
        init();
    }

    private void init() {
        map.put("paramName", this.paramName);
        map.put("paramRemark", this.paramRemark);
        map.put("paramType", this.type.getParamTypeName());
        map.put("bigName", CaseFormat.LOWER_CAMEL.to(CaseFormat.UPPER_CAMEL, this.paramName));
        map.put("columnName", this.columnName);
        map.put("columnType", this.type.getColumnTypeName());
    }

    public MyType getType() {
        return type;
    }

    public void setType(MyType type) {
        this.type = type;
    }

    public String getParamName() {
        return paramName;
    }

    public void setParamName(String paramName) {
        this.paramName = paramName;
    }

    public String getParamRemark() {
        return paramRemark;
    }

    public void setParamRemark(String paramRemark) {
        this.paramRemark = paramRemark;
    }

    public Map<String, String> getMap() {
        return map;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

}

class MyType {

    private String paramTypeName;

    private String columnTypeName;

    private int value;

    private int precision;

    private int scale;

    public MyType(int dateType, int columnSize, int decimalDigits) {
        this.value = dateType;
        if (Types.DECIMAL == dateType) {
            if (isInteger(columnSize, decimalDigits)) {
                // 有时候有可能会出现解析失去精度
                paramTypeName = "Integer";
                columnTypeName = "NUMBER";
            } else {
                paramTypeName = "Double";
                columnTypeName = "NUMBER(" + columnSize + "," + decimalDigits + ")";
            }
        } else if (Types.VARCHAR == dateType) {
            paramTypeName = "String";
            columnTypeName = "VARCHAR2(" + columnSize + ")";
        } else if (Types.TIMESTAMP == dateType) {
            paramTypeName = "Date";
            // columnTypeName = "TIMESTAMP";
            columnTypeName = "DATE";
        } else if (Types.DATE == dateType) {
            paramTypeName = "Date";
            columnTypeName = "DATE";
        } else if (Types.CHAR == dateType) {
            paramTypeName = "String";
            columnTypeName = "CHAR(" + columnSize + ")";
        } else {
            throw new RuntimeException("未知类型！");
        }
    }

    private boolean isInteger(int columnSize, int decimalDigits) {
        return decimalDigits == 0 || decimalDigits == -127 && columnSize == 0;
    }

    public MyType(String paramTypeName) {
        super();
        this.paramTypeName = paramTypeName;
        this.value = Types.JAVA_OBJECT;
        this.columnTypeName = "Java对象";
    }

    public String getParamTypeName() {
        return paramTypeName;
    }

    public void setParamTypeName(String paramTypeName) {
        this.paramTypeName = paramTypeName;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName) {
        this.columnTypeName = columnTypeName;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }

    public int getPrecision() {
        return precision;
    }

    public void setPrecision(int precision) {
        this.precision = precision;
    }

    public int getScale() {
        return scale;
    }

    public void setScale(int scale) {
        this.scale = scale;
    }
}
