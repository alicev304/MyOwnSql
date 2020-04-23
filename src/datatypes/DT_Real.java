package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;

public class DT_Real extends DT_Numeric<Float> {

    public DT_Real() {
        this(0, true);
    }

    public DT_Real(Float value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_Real(float value, boolean isNull) {
        super(Constants.REAL_SERIAL_TYPE_CODE, Constants.FOUR_BYTE_NULL_SERIAL_TYPE_CODE, Float.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    @Override
    public void increment(Float value) {
        this.value += value;
    }

    @Override
    public boolean compare(DT_Numeric<Float> object2, short condition) {
        if(value == null) return false;
        switch (condition) {
            case DT_Numeric.EQUALS:
                return Float.floatToIntBits(value) == Float.floatToIntBits(object2.getValue());

            case DT_Numeric.GREATER_THAN:
                return value > object2.getValue();

            case DT_Numeric.LESS_THAN:
                return value < object2.getValue();

            case DT_Numeric.GREATER_THAN_EQUALS:
                return Float.floatToIntBits(value) >= Float.floatToIntBits(object2.getValue());

            case DT_Numeric.LESS_THAN_EQUALS:
                return Float.floatToIntBits(value) <= Float.floatToIntBits(object2.getValue());

            default:
                return false;
        }
    }

    public boolean compare(DT_Double object2, short condition) {
        DT_Double object = new DT_Double(value, false);
        return object.compare(object2, condition);
    }
}
