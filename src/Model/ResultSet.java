package Model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class ResultSet extends Result{
    private ArrayList<String> columns;
    private ArrayList<Record> records;

    public ResultSet(int rowAffected) {
        super(rowAffected);
        this.records = new ArrayList<>();
    }

    public static ResultSet CreateResultSet(){
        return new ResultSet(0);
    }

    public void addRecord(Record record){
        if(record == null) return;

        if(this.records == null) {
            this.records = new ArrayList<>();
        }

        this.records.add(record);
        this.rowAffected++;
    }

    public void setColumns(ArrayList<String> columns){
        this.columns = columns;
    }

    @Override
    public void Display(){
        if(this.columns == null && this.columns.size() == 0) return;
        HashMap<String, Integer> columnSizeMap = new HashMap<>();

        if(this.records == null || this.records.size() == 0){
            System.out.println("Empty Set");
            return;
        }

        for(String column : this.columns){
            int maxLength = column.length();
            for(Record record : records){
                if(record.valueMap.containsKey(column)){
                    String value = record.valueMap.get(column).toString();
                    if(value.length() > maxLength){
                        maxLength = value.length();
                    }
                }
            }

            columnSizeMap.put(column, maxLength);
        }

        String boundary = DisplayLine(columns, columnSizeMap);
        System.out.println(boundary);
        String columns = DisplayColumns(columnSizeMap);
        System.out.println(columns);
        System.out.println(boundary);

        for(Record record : this.records) {
            String recordString = DisplayRecord(record, this.columns, columnSizeMap);
            System.out.println(recordString);
        }

        System.out.println(boundary);
        System.out.println(String.format("%d rows in set", this.rowAffected));
        System.out.println();
    }
    

    private String DisplayColumns(HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : this.columns){
            buffer.append(column);
            int size = columnSizeMap.get(column);
            String filler = FillerString(' ', size-column.length() + 4);
            buffer.append(filler);
        }

        return buffer.toString();
    }

    private String DisplayRecord(Record record, ArrayList<String> columns, HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : this.columns){
            if(record.valueMap.containsKey(column)) {
                String value = record.get(column);
                buffer.append(value);
                int size = columnSizeMap.get(column);
                String filler = FillerString(' ', size - value.length() + 4);
                buffer.append(filler);
            }
        }
        
        return buffer.toString();
    }


    private String DisplayLine(ArrayList<String> columns, HashMap<String, Integer> columnSizeMap) {
        StringBuffer buffer = new StringBuffer();

        for(String column : columns){
            String filler = FillerString('-', columnSizeMap.get(column) + 4);
            buffer.append(filler);
        }

        return buffer.toString();
    }

    private String FillerString(char character, int size) {
        char[] repeatCharacters = new char[size];
        Arrays.fill(repeatCharacters, character);
        return new String(repeatCharacters);
    }
}
