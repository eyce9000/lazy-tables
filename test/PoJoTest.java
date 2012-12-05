import java.io.File;
import java.io.IOException;

import com.grl.tables.Table;
import com.grl.tables.annotations.TableColumn;
import com.grl.tables.storage.CSVStorage;


public class PoJoTest {
	public static void main(String[] args) throws IOException{
		Object testObj = new Object(){
			@TableColumn(name="Column Header")
			private String testValue = "Something";
		};
		Table.Row row = new Table.Row(testObj);
		Table table = new Table();
		table.appendRow(row);
		
		CSVStorage storage = new CSVStorage();
		storage.storeTable(new File("test.csv"), table, true);
	}
}
