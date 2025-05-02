declare $vrec4 MAP( RECORD( age LONG, friends ARRAY(STRING) ) );

select * from Users where Users.children.values().friends[] =any $vrec4.keys()
