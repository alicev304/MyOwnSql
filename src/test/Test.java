package test;

import common.Constants;
import common.Utils;
import datatypes.DT_Text;
import datatypes.base.DT;
import datatypes.base.DT_Numeric;
import storage.StorageManager;
import storage.model.DataRecord;

import java.util.ArrayList;
import java.util.List;

public class Test {

    public void run(int numberOfTestCases) {
        switch (numberOfTestCases) {
            case 1:
                fetchTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 2:
                fetchSelectiveTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 3:
                selectAll(Constants.SYSTEM_TABLES_TABLENAME);
                break;

            case 4:
                deleteTableName(Constants.SYSTEM_COLUMNS_TABLENAME);

            case 5:
                deleteTableColumns(Constants.SYSTEM_COLUMNS_TABLENAME);
        }
    }

    public void fetchTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        columnIndexList.add((byte) 3);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        valueList.add(new DT_Text("TEXT"));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        conditionList.add(DT_Numeric.EQUALS);
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    public void fetchSelectiveTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        List<Byte> selectionIndexList = new ArrayList<>();
        selectionIndexList.add((byte) 0);
        selectionIndexList.add((byte) 5);
        selectionIndexList.add((byte) 2);
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList, selectionIndexList, false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void selectAll(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        List<DataRecord> records = manager.findRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,false);
        for (DataRecord record : records) {
            for(Object object: record.getColumnValueList()) {
                System.out.print(((DT) object).getValue());
                System.out.print("    |    ");
            }
            System.out.print("\n");
        }
    }

    private void deleteTableName(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        columnIndexList.add((byte) 1);
        List<Object> valueList = new ArrayList<>();
        valueList.add(new DT_Text(tableName));
        List<Short> conditionList = new ArrayList<>();
        conditionList.add(DT_Numeric.EQUALS);
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME, columnIndexList, valueList, conditionList,true));
    }

    private void deleteTableColumns(String tableName) {
        StorageManager manager = new StorageManager();
        List<Byte> columnIndexList = new ArrayList<>();
        List<Object> valueList = new ArrayList<>();
        List<Short> conditionList = new ArrayList<>();
        System.out.println(manager.deleteRecord(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME, columnIndexList, valueList, conditionList,false));
    }
}
