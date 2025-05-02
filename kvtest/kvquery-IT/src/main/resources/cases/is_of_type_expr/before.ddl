CREATE TABLE Foo(
    id  INTEGER,
    int  INTEGER,
    lng  LONG,
    dbl  DOUBLE,
    num  NUMBER,
    flt  FLOAT,
    str  STRING,
    bool BOOLEAN,
    enm  ENUM(tok1, tok2, tok3, tok4),
    arr  JSON,
    map    MAP(ARRAY(INTEGER)),
    rec    RECORD(fmap MAP(ARRAY(FLOAT))),
    arrrec ARRAY(RECORD(fmap MAP(ARRAY(FLOAT)))),
    primary key (id)
)
