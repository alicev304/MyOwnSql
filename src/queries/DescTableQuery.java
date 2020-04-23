package queries;

import Model.Condition;
import Model.IQuery;
import Model.Result;
import Model.ResultSet;
import QueryParser.DatabaseHelper;
import common.Constants;
import common.Utils;

import java.util.ArrayList;

public class DescTableQuery implements IQuery {

    public String databaseName;
    public String tableName;

    public DescTableQuery(String databaseName, String tableName) {
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public Result ExecuteQuery() {

        ArrayList<String> columns = new ArrayList<>();
        columns.add("column_name");
        columns.add("data_type");
        columns.add("column_key");
        columns.add("is_nullable");

        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);
        condition = Condition.CreateCondition(String.format("table_name = '%s'", this.tableName));
        conditionList.add(condition);

        IQuery query = new SelectQuery(Constants.DEFAULT_CATALOG_DATABASENAME, Constants.SYSTEM_COLUMNS_TABLENAME, columns, conditionList, false);
        if(query.ValidateQuery()) {
            ResultSet resultSet = (ResultSet) query.ExecuteQuery();
            return resultSet;
        }

        return null;
    }

    @Override
    public boolean ValidateQuery() {
        boolean tableExists = DatabaseHelper.TableExists(this.databaseName, this.tableName);

        if(!tableExists){
            Utils.printError(String.format("Unknown table '%s.%s'", this.databaseName, this.tableName));
            return false;
        }

        return true;
    }
}
