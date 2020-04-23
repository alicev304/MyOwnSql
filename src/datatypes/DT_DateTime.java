package datatypes;

import common.Constants;
import datatypes.base.DT_Numeric;

import java.util.Date;

public class DT_DateTime extends DT_Numeric<Long> {

    public DT_DateTime() {
        this(0, true);
    }

    public DT_DateTime(Long value) {
        this(value == null ? 0 : value, value == null);
    }

    public DT_DateTime(long value, boolean isNull) {
        super(Constants.DATE_TIME_SERIAL_TYPE_CODE, Constants.EIGHT_BYTE_NULL_SERIAL_TYPE_CODE, Long.BYTES);
        this.value = value;
        this.isNull = isNull;
    }

    public String getStringValue() {
        Date date = new Date(this.value);
        return date.toString();
    }

    @Override
    public void increment(Long value) {
        this.value += value;
    }

    @Override
    public boolean compare(DT_Numeric<Long> object2, short condition) {
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
}
