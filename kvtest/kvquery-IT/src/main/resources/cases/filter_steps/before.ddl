###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE Complex(
  id INTEGER,
  firstName STRING not null default "somename",
  lastName STRING,
  age INTEGER not null default 20,
  ptr STRING,
  address RECORD( city STRING not null default "Portland",
                  state STRING not null default "OR",
                  phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                  ptr STRING),
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)


create table foo(
  id INTEGER,
  complex1 RECORD(map MAP(ARRAY(MAP(INTEGER)))),
  complex2 RECORD(arr ARRAY(ARRAY(RECORD(a LONG, b LONG)))),
  primary key(id)
)

