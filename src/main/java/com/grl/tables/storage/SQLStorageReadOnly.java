package com.grl.tables.storage;

import java.sql.Connection;
import java.sql.SQLException;

import com.grl.tables.Table;

public class SQLStorageReadOnly extends SQLStorage{

	public SQLStorageReadOnly(Connection conn) {
		super(conn);
	}

	@Override
	public void createTable(Table table, String schemaName, String tableName)
			throws SQLException {
		// TODO Auto-generated method stub
		
	}


}
