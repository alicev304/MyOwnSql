package datatypes.base;

import Model.Literal;
import common.Constants;
import common.Utils;
import datatypes.*;

public abstract class DT<T> {
	
	  protected final byte valueSerialCode;

	    protected final byte nullSerialCode;

    protected T value;

    protected boolean isNull;

  

    public static DT CreateDT(Literal value) {
        switch(value.type) {
            case TINYINT:
                return new DT_TinyInt(Byte.valueOf(value.value));
            case SMALLINT:
                return new DT_SmallInt(Short.valueOf(value.value));
            case BIGINT:
                return new DataTypeInt(Long.valueOf(value.value));
            case INT:
                return new DT_Int(Integer.valueOf(value.value));
            case REAL:
                return new DT_Real(Float.valueOf(value.value));
            case DOUBLE:
                return new DT_Double(Double.valueOf(value.value));
            case DATETIME:
                return new DT_DateTime();
            case DATE:
                return new DT_Date();
            case TEXT:
                return new DT_Text(value.value);
        }

        return null;
    }

    public static DT createSystemDT(String value, byte dataType) {
        switch(dataType) {
            case Constants.TINYINT:
                return new DT_TinyInt(Byte.valueOf(value));
            case Constants.SMALLINT:
                return new DT_SmallInt(Short.valueOf(value));
            case Constants.BIGINT:
                return new DataTypeInt(Long.valueOf(value));
            case Constants.INT:
                return new DT_Int(Integer.valueOf(value));
            case Constants.REAL:
                return new DT_Real(Float.valueOf(value));
            case Constants.DOUBLE:
                return new DT_Double(Double.valueOf(value));
            case Constants.DATETIME:
                return new DT_DateTime(Utils.getDateEpoc(value, false));
            case Constants.DATE:
                return new DT_DateTime(Utils.getDateEpoc(value, true));
            case Constants.TEXT:
                return new DT_Text(value);
        }

        return null;
    }

    protected DT(int valueSerialCode, int nullSerialCode) {
        this.valueSerialCode = (byte) valueSerialCode;
        this.nullSerialCode = (byte) nullSerialCode;
    }

    public T getValue() {
        return value;
    }
    
    public void setNull(boolean aNull) {
        isNull = aNull;
    }

    public byte getValueSerialCode() {
        return valueSerialCode;
    }

    public byte getNullSerialCode() {
        return nullSerialCode;
    }

    public String getStringValue() {
        if(value == null) {
            return "NULL";
        }
        return value.toString();
    }

    public void setValue(T value) {
        this.value = value;
         if (value != null) {
             this.isNull = false;
         }
    }

    public boolean isNull() {
        return isNull;
    }

   
}
