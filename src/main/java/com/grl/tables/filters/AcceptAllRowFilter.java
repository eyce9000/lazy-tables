package com.grl.tables.filters;

import java.util.Map;

public class AcceptAllRowFilter implements RowFilter {

	@Override
	public boolean acceptsRow(Map<String, String> row) {
		return true;
	}

}
