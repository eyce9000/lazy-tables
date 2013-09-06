package com.grl.tables.events;

import com.grl.tables.Table.Row;

public interface RowReadListener {
	public void readStart();
	public void onRowRead(Row row);
	public void readComplete();
}
