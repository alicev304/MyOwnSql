package helpers;

public interface RegexExpressions {

    String STATEMENT_VALIDATION_REGEX = "(?i)(show|create|drop|insert into|update|delete from|select|exit|help)";
    String CREATE_STATEMENT_VALIDATION_REGEX = "(?i)create (table|database)";
    String CREATE_TABLE_STATEMENT_VALIDATION_REGEX = "(?i)create table[\\s]+(.+)[\\s]+\\(";
}
