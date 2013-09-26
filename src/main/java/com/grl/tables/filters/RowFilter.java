package com.grl.tables.filters;

import java.util.Map;

public interface RowFilter {
	public boolean acceptsRow(Map<String,String> row);
}
