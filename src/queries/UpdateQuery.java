package queries;

import Model.Condition;
import Model.IQuery;
import Model.Literal;
import Model.Result;
import common.Constants;
import common.Utils;
import datatypes.base.DT;
import storage.StorageManager;

import java.util.*;

public class UpdateQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public String columnName;
    public Literal value;
    public Condition condition;

    public UpdateQuery(String databaseName, String tableName, String columnName, Literal value, Condition condition){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columnName = columnName;
        this.value = value;
        this.condition = condition;
    }

    @Override
    public Result ExecuteQuery() {
             StorageManager manager = new StorageManager();

        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);
        List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
        List<Byte> searchColumnsIndexList = getSearchColumnsIndexList(retrievedColumns);
        List<Object> searchKeysValueList = getSearchKeysValueList(columnDataTypeMapping);
        List<Short> searchKeysConditionsList = getSearchKeysConditionsList(retrievedColumns);
        List<Byte> updateColumnIndexList = getUpdateColumnIndexList(retrievedColumns);
        List<Object> updateColumnValueList = getUpdateColumnValueList(columnDataTypeMapping);

        int rowsAffected = manager.updateRecord(Utils.getUserDatabasePath(databaseName), tableName, searchColumnsIndexList, searchKeysValueList, searchKeysConditionsList, updateColumnIndexList, updateColumnValueList, false);

        Result result;
        result = new Result(rowsAffected);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        StorageManager manager = new StorageManager();
        List<String> retrievedColumns = manager.fetchAllTableColumns(tableName);
        HashMap<String, Integer> columnDataTypeMapping = manager.fetchAllTableColumndataTypes(tableName);

        if (!manager.checkTableExists(Utils.getUserDatabasePath(this.databaseName), tableName)) {
            Utils.printMissingTableError(tableName);
            return false;
        }

        if (this.condition == null) {
           if(!checkColumnValidity(retrievedColumns, false)) {
                return false;
            }

            if(!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, false)) {
                return false;
            }

            return true;
        }
        else {
            if(!checkColumnValidity(retrievedColumns, true)) {
                return false;
            }

            if(!checkColumnValidity(retrievedColumns, false)) {
                return false;
            }

            if(!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, true)) {
                return false;
            }

            if(!checkValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, false)) {
                return false;
            }
        }

        return true;
    }

    private boolean checkValueDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping, List<String> columnsList, boolean isConditionCheck) {
        String invalidColumn = "";

        String column = isConditionCheck ? condition.column : columnName;
        Literal columnValue = isConditionCheck ? condition.value : value;

        if (columnsList.contains(column)) {
            int dataTypeIndex = columnDataTypeMapping.get(column);
            Literal literal = columnValue;

            if (dataTypeIndex != Constants.INVALID_CLASS && dataTypeIndex <= Constants.DOUBLE) {
                if (!Utils.canConvertStringToDouble(literal.value)) {
                    invalidColumn = column;
                }
            }
            else if (dataTypeIndex == Constants.DATE) {
                if (!Utils.isvalidDateFormat(literal.value)) {
                    invalidColumn = column;
                }
            }
            else if (dataTypeIndex == Constants.DATETIME) {
                if (!Utils.isvalidDateTimeFormat(literal.value)) {
                    invalidColumn = column;
                }
            }
        }

        boolean valid = (invalidColumn.length() > 0) ? false : true;
        if (!valid) {
            Utils.printError("The value of the column " + invalidColumn + " is invalid.");

        }

        return valid;
    }

    private boolean checkColumnValidity(List<String> retrievedColumns, boolean isConditionCheck) {
        boolean columnsValid = true;
        String invalidColumn = "";

        String tableColumn = isConditionCheck ? condition.column : columnName;
        if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
            columnsValid = false;
            invalidColumn = tableColumn;
        }

        if (!columnsValid) {
            Utils.printError("Column " + invalidColumn + " is not present in the table " + tableName + ".");
            return false;
        }

        return true;
    }



    private List<Byte> getSearchColumnsIndexList(List<String>retrievedList) {
        List<Byte> list = new ArrayList<>();
        if (condition != null) {
            int idx = retrievedList.indexOf(condition.column);
            list.add((byte)idx);
        }

        return list;
    }

    private List<Object> getSearchKeysValueList(HashMap<String, Integer> columnDataTypeMapping) {
        List<Object> list = new ArrayList<>();
        if (condition != null) {
            byte dataTypeIndex = (byte)columnDataTypeMapping.get(this.condition.column).intValue();
            DT dataType = DT.createSystemDT(this.condition.value.value, dataTypeIndex);
            list.add(dataType);
        }

        return list;
    }

    private List<Short> getSearchKeysConditionsList(List<String>retrievedList) {
        List<Short> list = new ArrayList<>();
        if (condition != null) {
            list.add(Utils.ConvertFromOperator(condition.operator));
        }

        return list;
    }

    private List<Byte> getUpdateColumnIndexList(List<String>retrievedList) {
        List<Byte> list = new ArrayList<>();
        int idx = retrievedList.indexOf(columnName);
        list.add((byte)idx);

        return list;
    }

    private List<Object> getUpdateColumnValueList(HashMap<String, Integer> columnDataTypeMapping) {
        List<Object> list = new ArrayList<>();
        byte dataTypeIndex = (byte) columnDataTypeMapping.get(columnName).intValue();

        DT dataType = DT.createSystemDT(value.value, dataTypeIndex);
        list.add(dataType);

        return list;
    }
}
