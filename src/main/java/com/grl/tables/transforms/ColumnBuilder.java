package com.grl.tables.transforms;

import java.util.Map;

public interface ColumnBuilder {
	public String buildColumn(Map<String,String> rawData);
}
