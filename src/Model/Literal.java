package Model;

import QueryParser.DatabaseHelper;
import common.Constants;
import common.Utils;
import datatypes.base.DT;

public class Literal {
    public DataType type;
    public String value;

    public static Literal CreateLiteral(DT value, Byte type) {
        if(type == Constants.INVALID_CLASS) return null;

        switch(type) {
            case Constants.TINYINT:
                return new Literal(DataType.TINYINT, value.getStringValue());
            case Constants.SMALLINT:
                return new Literal(DataType.SMALLINT, value.getStringValue());
            case Constants.INT:
                return new Literal(DataType.INT, value.getStringValue());
            case Constants.BIGINT:
                return new Literal(DataType.BIGINT, value.getStringValue());
            case Constants.REAL:
                return new Literal(DataType.REAL, value.getStringValue());
            case Constants.DOUBLE:
                return new Literal(DataType.DOUBLE, value.getStringValue());
            case Constants.DATE:
                return new Literal(DataType.DATE, Utils.getDateEpocAsString((long)value.getValue(), true));
            case Constants.DATETIME:
                return new Literal(DataType.DATETIME, Utils.getDateEpocAsString((long)value.getValue(), false));
            case Constants.TEXT:
                return new Literal(DataType.TEXT, value.getStringValue());
        }

        return null;
    }

    public static Literal CreateLiteral(String literalString){
        if(literalString.startsWith("'") && literalString.endsWith("'")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literal(DataType.TEXT, literalString);
        }

        if(literalString.startsWith("\"") && literalString.endsWith("\"")){
            literalString = literalString.substring(1, literalString.length()-1);
            return new Literal(DataType.TEXT, literalString);
        }

        try{
            Integer.parseInt(literalString);
            return new Literal(DataType.INT, literalString);
        }
        catch (Exception e){}

        try{
            Double.parseDouble(literalString);
            return new Literal(DataType.REAL, literalString);
        }
        catch (Exception e){}

            DatabaseHelper.UnknownCommand(literalString, "Unrecognised Literal Found. Please use integers, real or strings ");
        return null;
    }

    private Literal(DataType type, String value) {
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        if (this.type == DataType.TEXT) {
            return this.value;
        } else if (this.type == DataType.INT || this.type == DataType.TINYINT ||
                this.type == DataType.SMALLINT || this.type == DataType.BIGINT) {
            return this.value;
        } else if (this.type == DataType.REAL || this.type == DataType.DOUBLE) {
            return String.format("%.2f", Double.parseDouble(this.value));
        } else if (this.type == DataType.INT_REAL_NULL || this.type == DataType.SMALL_INT_NULL || this.type == DataType.TINY_INT_NULL || this.type == DataType.DOUBLE_DATETIME_NULL) {
            return "NULL";
        } else if (this.type == DataType.DATE || this.type == DataType.DATETIME) {
            return this.value;
        }

        return "";
    }
}
