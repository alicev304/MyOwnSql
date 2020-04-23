package common;

import Model.Condition;
import Model.Literal;
import Model.Operator;
import datatypes.*;
import datatypes.base.DT_Numeric;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

public class Utils {

    public static String getSystemDatabasePath() {
        return Constants.DEFAULT_DATA_DIRNAME + "/" + Constants.DEFAULT_CATALOG_DATABASENAME;
    }

    public static String getUserDatabasePath(String database) {
        return Constants.DEFAULT_DATA_DIRNAME + "/" + database;
    }

    public static void printError(String errorMessage) {
        printMessage(errorMessage);
    }

    public static void printMissingDatabaseError(String databaseName) {
        printError("The database '" + databaseName + "' does not exist");
    }

    public static void printMissingTableError(String tableName) {
        printError("Table '" + tableName + "' doesn't exist.");
    }

    public static void printDuplicateTableError(String tableName) {
        printError("Table '" + tableName + "' already exist.");
    }

    public static void printMessage(String str) {
        System.out.println(str);
    }

    public static void printUnknownColumnValueError(String value) {
        printMessage("Unknown column value '" + value + "' in 'value list'");
    }

    public static void printUnknownConditionValueError(String value) {
        printMessage("Unknown column value '" + value + "' in 'value list'");
    }

    public static byte resolveClass(Object object) {
        if(object.getClass().equals(DT_TinyInt.class)) {
            return Constants.TINYINT;
        }
        else if(object.getClass().equals(DT_SmallInt.class)) {
            return Constants.SMALLINT;
        }
        else if(object.getClass().equals(DT_Int.class)) {
            return Constants.INT;
        }
        else if(object.getClass().equals(DataTypeInt.class)) {
            return Constants.BIGINT;
        }
        else if(object.getClass().equals(DT_Real.class)) {
            return Constants.REAL;
        }
        else if(object.getClass().equals(DT_Double.class)) {
            return Constants.DOUBLE;
        }
        else if(object.getClass().equals(DT_Date.class)) {
            return Constants.DATE;
        }
        else if(object.getClass().equals(DT_DateTime.class)) {
            return Constants.DATETIME;
        }
        else if(object.getClass().equals(DT_Text.class)) {
            return Constants.TEXT;
        }
        else {
            return Constants.INVALID_CLASS;
        }
    }

    public static byte stringToDataType(String string) {
        if(string.compareToIgnoreCase("TINYINT") == 0) {
            return Constants.TINYINT;
        }
        else if(string.compareToIgnoreCase("SMALLINT") == 0) {
            return Constants.SMALLINT;
        }
        else if(string.compareToIgnoreCase("INT") == 0) {
            return Constants.INT;
        }
        else if(string.compareToIgnoreCase("BIGINT") == 0) {
            return Constants.BIGINT;
        }
        else if(string.compareToIgnoreCase("REAL") == 0) {
            return Constants.REAL;
        }
        else if(string.compareToIgnoreCase("DOUBLE") == 0) {
            return Constants.DOUBLE;
        }
        else if(string.compareToIgnoreCase("DATE") == 0) {
            return Constants.DATE;
        }
        else if(string.compareToIgnoreCase("DATETIME") == 0) {
            return Constants.DATETIME;
        }
        else if(string.compareToIgnoreCase("TEXT") == 0) {
            return Constants.TEXT;
        }
        else {
            return Constants.INVALID_CLASS;
        }
    }

    public static boolean canConvertStringToDouble(String value) {
        try {
            Double dVal = Double.parseDouble(value);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public static boolean isvalidDateFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            return false;
        }

        return true;
    }

    public static boolean isvalidDateTimeFormat(String date) {
        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setLenient(false);
        try {
            Date dateObj = formatter.parse(date);
        } catch (ParseException e) {
            
            return false;
        }

        return true;
    }

    public static Short ConvertFromOperator(Operator operator) {
        switch (operator){
            case EQUALS: return DT_Numeric.EQUALS;
            case GREATER_THAN_EQUAL: return DT_Numeric.GREATER_THAN_EQUALS;
            case GREATER_THAN: return DT_Numeric.GREATER_THAN;
            case LESS_THAN_EQUAL: return DT_Numeric.LESS_THAN_EQUALS;
            case LESS_THAN: return DT_Numeric.LESS_THAN;
        }

        return null;
    }

 
    public static String line(String s, int num) {
        String a = "";
        for(int i=0;i<num;i++) {
            a += s;
        }
        return a;
    }

    public static boolean checkConditionValueDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, Condition condition) {
        String invalidColumn = "";
        Literal literal = null;

        if (columnsList.contains(condition.column)) {
            int dataTypeIndex = columnDataTypeMapping.get(condition.column);
            literal = condition.value;

          
            if (dataTypeIndex != Constants.INVALID_CLASS && dataTypeIndex <= Constants.DOUBLE) {
                
                if (!Utils.canConvertStringToDouble(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            } else if (dataTypeIndex == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = condition.column;
                }
            }
        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printUnknownConditionValueError(literal.value);
        }

        return valid;
    }


    public static String getDateEpocAsString(long value, Boolean isDate) {
        ZoneId zoneId = ZoneId.of ( "America/Chicago" );

        Instant i = Instant.ofEpochSecond (value);
        ZonedDateTime zdt2 = ZonedDateTime.ofInstant (i, zoneId);
        Date date = Date.from(zdt2.toInstant());

        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        formatter.setLenient(false);

        String dateStr = formatter.format(date);
        return dateStr;
    }
    

    public static long getDateEpoc(String value, Boolean isDate) {
        DateFormat formatter = null;
        if (isDate) {
            formatter = new SimpleDateFormat("yyyy-MM-dd");
        }
        else {
            formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }
        formatter.setLenient(false);
        Date date;
        try {
            date = formatter.parse(value);

            ZonedDateTime zdt = ZonedDateTime.ofInstant(date.toInstant(),
                    ZoneId.systemDefault());

     
            return zdt.toInstant().toEpochMilli() / 1000;
        }
        catch (ParseException ex) {
            return 0;
        }
    }

    public boolean checkDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, List<Literal> values) {
        String invalidColumn = "";
        Literal invalidLiteral = null;

        for (String columnName : columnsList) {

           int dataTypeId = columnDataTypeMapping.get(columnName);

            int idx = columnsList.indexOf(columnName);
            Literal literal = values.get(idx);
            invalidLiteral = literal;
  if (dataTypeId != Constants.INVALID_CLASS && dataTypeId <= Constants.DOUBLE) {
                boolean isValid = Utils.canConvertStringToDouble(literal.value);
                if (!isValid) {
                    invalidColumn = columnName;
                    break;
                }
            }
            else if (dataTypeId == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            } else if (dataTypeId == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = columnName;
                    break;
                }
            }

        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printUnknownColumnValueError(invalidLiteral.value);
            return false;
        }

        return true;
    }
}
