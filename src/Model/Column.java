package Model;

import QueryParser.DatabaseHelper;

public class Column {
    public String name;
    public DataType type;
    public boolean isNull;

    public static Column CreateColumn(String columnString){
        String primaryKeyString = "primary key";
        String notNullString = "not null";
        boolean isNull = true;
        if(columnString.toLowerCase().endsWith(primaryKeyString)){
            columnString = columnString.substring(0, columnString.length() - primaryKeyString.length()).trim();
        }
        else if(columnString.toLowerCase().endsWith(notNullString)){
            columnString = columnString.substring(0, columnString.length() - notNullString.length()).trim();
            isNull = false;
        }

        String[] parts = columnString.split(" ");
        String name = "";
        if(parts.length > 2){
            DatabaseHelper.UnknownCommand(columnString, "Expected Column format <name> <datatype> [PRIMARY KEY]/[NOT NULL]");
            return null;
        }

        if(parts.length > 1){
            name = parts[0].trim();
            DataType type = GetDataType(parts[1].trim());
            if(type == null){
                DatabaseHelper.UnknownCommand(columnString, "Unrecognised Data type " + parts[1]);
                return null;
            }

            Column column = new Column(name, type, isNull);
            return column;
        }

        DatabaseHelper.UnknownCommand(columnString, "Expected Column format <name> <datatype> [PRIMARY KEY]/[NOT NULL]");
        return null;
    }

    private static DataType GetDataType(String dataTypeString) {
        switch(dataTypeString){
            case "tinyint": return DataType.TINYINT;
            case "smallint": return DataType.SMALLINT;
            case "int": return DataType.INT;
            case "bigint": return DataType.BIGINT;
            case "real": return DataType.REAL;
            case "double": return DataType.DOUBLE;
            case "datetime": return DataType.DATETIME;
            case "date": return DataType.DATE;
            case "text": return DataType.TEXT;
        }

        return null;
    }

    private Column(String name, DataType type, boolean isNull) {
        this.name = name;
        this.type = type;
        this.isNull = isNull;
    }
}
