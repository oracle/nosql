###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE sn (
  id        INTEGER,
  firstName STRING,
  age       INTEGER,
  long      LONG,
  num       NUMBER,
  double    DOUBLE,
  bool      BOOLEAN,
  float     FLOAT,
  bin       BINARY(5),
  bin1      BINARY,
  type      ENUM(working,student),
  time      timestamp(0),
  map       MAP(ARRAY(MAP(STRING))),
  array     ARRAY(MAP(ARRAY(INTEGER))),
  address   RECORD( city STRING,
                    state STRING,
                    phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                    ptr STRING),
  children  JSON,
  primary key (id)
)


