package com.jingsky.mv.maxwell.schema.columndef;

import com.jingsky.mv.maxwell.producer.MaxwellOutputConfig;

import java.sql.Date;
import java.util.Calendar;

public class YearColumnDef extends ColumnDef {
	public YearColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig outputConfig) {
		if ( value instanceof Date ) {
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(( Date ) value);
			return calendar.get(Calendar.YEAR);
		}
		return value;
	}

	@Override
	public String toSQL(Object value) {
		return ((Integer)value).toString();
	}

}
