package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;


public class DT_Int extends DT_Numeric<Integer> {

    public DT_Int() {
        this(0, true);
    }

    public DT_Int(Integer value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Int(int value, boolean isNull) {
        super(Constants.INT_SERIAL_TYPE_CODE, Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Integer.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Integer value) {
        this.value += value;
    }

    @Override
    public boolean compare(DT_Numeric<Integer> object2, short condition) {
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
        DT_Int object = new DT_Int(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DT_SmallInt object2, short condition) {
        DT_Int object = new DT_Int(object2.getValue(), false);
        return this.compare(object, condition);
    }

    public boolean compare(DataTypeInt object2, short condition) {
        DataTypeInt object = new DataTypeInt(value, false);
        return object.compare(object2, condition);
    }
}
