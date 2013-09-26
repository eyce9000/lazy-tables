package com.grl.tables.filters;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class ColumnsFilter implements ColumnFilter {
	Set<String> columns = new HashSet<String>();
	public ColumnsFilter(String...columnTitles){
		this(Arrays.asList(columnTitles));
	}
	public ColumnsFilter(Collection<String> columnTitles){
		columns.addAll(columnTitles);
	}
	@Override
	public boolean acceptsColumn(String columnName) {
		return columns.contains(columnName);
	}

}
