###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

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
    bin  BINARY,
    fbin  BINARY(10),
    address record(street string,
                 city string,
                 state string,
                 phone array(record(kind enum(work, home),
                                    areacode integer,
                                    number integer
                                   )
                            )
                ),
    longstr STRING,
    primary key (id1)
)

