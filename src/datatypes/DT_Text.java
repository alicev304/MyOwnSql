package datatypes;

import common.Constants;
import datatypes.base.DT;

public class DT_Text extends DT<String> {

    public DT_Text() {
        this("", true);
    }

    public DT_Text(String value) {
        this(value, value == null);
    }

    public DT_Text(String value, boolean isNull) {
        super(Constants.TEXT_SERIAL_TYPE_CODE, Constants.ONE_BYTE_NULL_SERIAL_TYPE_CODE);
        this.value = value;
        this.isNull = isNull;
    }

    public byte getSerialCode() {
        if(isNull)
            return nullSerialCode;
        else
            return (byte)(valueSerialCode + this.value.length());
    }

    public int getSize() {
        if(isNull)
            return 0;
        return this.value.length();
    }
}
