create table if not exists stringsTable (
    id integer, str1 string, str2 string, str3 string, primary key(id))


create table if not exists stringsTable2 (
    id integer,
    str string,
    jsn json,
    arrStr    ARRAY(string),
    arrInt    ARRAY(INTEGER),
    mapStr    MAP(ARRAY(STRING)),
    mapInt    MAP(ARRAY(INTEGER)),
    recStr    RECORD(fmap MAP(ARRAY(string))),
    recFlt    RECORD(fmap MAP(ARRAY(FLOAT))),
    arrrecStr ARRAY(RECORD(fmap MAP(ARRAY(string)))),
    arrrecFlt ARRAY(RECORD(fmap MAP(ARRAY(FLOAT)))),
    primary key(id))

