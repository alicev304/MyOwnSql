package queries;

import Model.*;

import java.io.File;
import java.util.ArrayList;


public class ShowDatabaseQuery implements IQuery {
    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("Database");
        ResultSet resultSet = ResultSet.CreateResultSet();
        resultSet.setColumns(columns);
        ArrayList<Record> records = DummyData();

        for(Record record : records){
            resultSet.addRecord(record);
        }

        return resultSet;
    }

    @Override
    public boolean ValidateQuery() {
        return true;
    }

    private ArrayList<Record> DummyData(){
        ArrayList<Record> records = new ArrayList<>();

        String DEFAULT_DATA_DIRNAME = "data";
        File baseData = new File(DEFAULT_DATA_DIRNAME);

        for(File data : baseData.listFiles()){
            if(!data.isDirectory()) continue;
            Record record = Record.CreateRecord();
            record.put("Database", Literal.CreateLiteral(String.format("\"%s\"", data.getName())));
            records.add(record);
        }

        return records;
    }
}
