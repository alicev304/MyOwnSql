package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;

public class DT_SmallInt extends DT_Numeric<Short> {

    public DT_SmallInt() {
        this((short) 0, true);
    }

    public DT_SmallInt(Short value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_SmallInt(short value, boolean isNull) {
        super(Constants.SMALL_INT_SERIAL_TYPE_CODE, Constants.TWO_BYTE_NULL_SERIAL_TYPE_CODE, Short.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Short value) {
        this.value = (short)(this.value + value);
    }

    @Override
    public boolean compare(DT_Numeric<Short> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DT_Numeric.EQUALS:
                return value == object2.getValue();

            case DT_Numeric.GREATER_THAN:
                return value > object2.getValue();

            case DT_Numeric.LESS_THAN:
                return value < object2.getValue();

            case DT_Numeric.GREATER_THAN_EQUALS:
                return value >= object2.getValue();

            case DT_Numeric.LESS_THAN_EQUALS:
                return value <= object2.getValue();

            default:
                return false;
        }
    }

    public boolean compare(DT_TinyInt object2, short condition) {
        DT_SmallInt object = new DT_SmallInt(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DT_Int object2, short condition) {
        DT_Int object = new DT_Int(value, false);
        return object.compare(object2, condition);
    }

    public boolean compare(DataTypeInt object2, short condition) {
        DataTypeInt object = new DataTypeInt(value, false);
        return object.compare(object2, condition);
    }
}
