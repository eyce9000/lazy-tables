package com.grl.tables.filters;

public class AcceptAllColumnFilter implements ColumnFilter{

	@Override
	public boolean acceptsColumn(String columnName) {
		return true;
	}

}
