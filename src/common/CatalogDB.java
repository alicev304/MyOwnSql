package common;

import Model.DataType;
import helpers.UpdateStatementHelper;
import storage.StorageManager;
import storage.model.InternalColumn;

import java.util.ArrayList;
import java.util.List;

public class CatalogDB {

    public static final byte TABLES_TABLE_SCHEMA_ROWID = 0;
    public static final byte TABLES_TABLE_SCHEMA_DATABASE_NAME = 1;
    public static final byte TABLES_TABLE_SCHEMA_TABLE_NAME = 2;
    public static final byte TABLES_TABLE_SCHEMA_RECORD_COUNT = 3;
    public static final byte TABLES_TABLE_SCHEMA_COL_TBL_ST_ROWID = 4;
    public static final byte TABLES_TABLE_SCHEMA_NXT_AVL_COL_TBL_ROWID = 5;


    public static final byte COLUMNS_TABLE_SCHEMA_ROWID = 0;
    public static final byte COLUMNS_TABLE_SCHEMA_DATABASE_NAME = 1;
    public static final byte COLUMNS_TABLE_SCHEMA_TABLE_NAME = 2;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_NAME = 3;
    public static final byte COLUMNS_TABLE_SCHEMA_DATA_TYPE = 4;
    public static final byte COLUMNS_TABLE_SCHEMA_COLUMN_KEY = 5;
    public static final byte COLUMNS_TABLE_SCHEMA_ORDINAL_POSITION = 6;
    public static final byte COLUMNS_TABLE_SCHEMA_IS_NULLABLE = 7;

    public static final String PRIMARY_KEY_IDENTIFIER = "PRI";

    public boolean createCatalogDB() {
        StorageManager manager = new StorageManager();
        UpdateStatementHelper statement = new UpdateStatementHelper();
        manager.createTable(Utils.getSystemDatabasePath(), Constants.SYSTEM_TABLES_TABLENAME + Constants.DEFAULT_FILE_EXTENSION);
        manager.createTable(Utils.getSystemDatabasePath(), Constants.SYSTEM_COLUMNS_TABLENAME + Constants.DEFAULT_FILE_EXTENSION);
        int startingRowId = statement.updateSystemTablesTable(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, 6);
        startingRowId *= statement.updateSystemTablesTable(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, 8);
        if(startingRowId >= 0) {
            List<InternalColumn> columns = new ArrayList<>();
            columns.add(new InternalColumn("rowid", DataType.INT.toString(), false, false));
            columns.add(new InternalColumn("database_name", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("table_name", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("record_count", DataType.INT.toString(), false, false));
            columns.add(new InternalColumn("col_tbl_st_rowid", DataType.INT.toString(), false, false));
            columns.add(new InternalColumn("nxt_avl_col_tbl_rowid", DataType.INT.toString(), false, false));
            statement.updateSystemColumnsTable(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_TABLES_TABLENAME, 1, columns);
            columns.clear();
            columns.add(new InternalColumn("rowid", DataType.INT.toString(), false, false));
            columns.add(new InternalColumn("database_name", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("table_name", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("column_name", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("data_type", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("column_key", DataType.TEXT.toString(), false, false));
            columns.add(new InternalColumn("ordinal_position", DataType.TINYINT.toString(), false, false));
            columns.add(new InternalColumn("is_nullable", DataType.TEXT.toString(), false, false));
            statement.updateSystemColumnsTable(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, 7, columns);
        }
        return true;
    }
}

