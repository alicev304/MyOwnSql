package queries;

import Model.IQuery;
import Model.Result;
import QueryParser.DatabaseHelper;
import common.Constants;

import java.io.File;

public class CreateDatabaseQuery implements IQuery {
    public String databaseName;

    public CreateDatabaseQuery(String databaseName){
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        File database = new File(Constants.DEFAULT_DATA_DIRNAME + "/" + this.databaseName);
        boolean isCreated = database.mkdir();

        if(!isCreated){
            System.out.println(String.format("Unable to create database '%s'", this.databaseName));
            return null;
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = DatabaseHelper.DatabaseExists(this.databaseName);

        if(databaseExists){
            System.out.println(String.format("Database '%s' already exists", this.databaseName));
            return false;
        }

        return true;
    }
}
