package org.nyxdb.parser.core.parallel;

public enum QueryType {
    CREATE_TABLE,
    CREATE_DATABASE,
    DROP_TABLE,
    DROP_DATABASE,
    INSERT,
    UPDATE,
    DELETE,
    SELECT,
    ALTER_TABLE,
    TRUNCATE_TABLE,
    UNKNOWN
}
