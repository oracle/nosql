#Test Description: Test for external variable of type map of records.

declare $vrec5 MAP( RECORD( age LONG, friends ARRAY(STRING) ) );

select * from Users where Users.children.values().friends[] =any $vrec5.keys()
