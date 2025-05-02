CREATE TABLE Foo(
    id1  INTEGER,
    int  INTEGER,
    lng  LONG,
    dbl  DOUBLE,
    flt  FLOAT,
    str  STRING,
    bool BOOLEAN,
    enm  ENUM(tok1, tok2, tok3, tok4),
    arr    ARRAY(INTEGER),
    map    MAP(ARRAY(INTEGER)),
    rec    RECORD(fmap MAP(ARRAY(FLOAT))),
    arrrec ARRAY(RECORD(fmap MAP(ARRAY(FLOAT)))),
    arrrec2 ARRAY(RECORD(a INTEGER)),
    primary key (id1)
)


create table connections(id integer, connections array(integer), primary key(id))
