###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(
   id INTEGER,
   rec RECORD(a INTEGER,
              b ARRAY(INTEGER),
              c ARRAY(RECORD(ca INTEGER, cb integer)),
              d ARRAY(MAP(INTEGER)),
              f FLOAT),
              primary key(id))

CREATE INDEX idx_a_c_f ON Foo (rec.a, rec.c[].ca, rec.f)

CREATE INDEX idx_d_f ON Foo (rec.d[].d2, rec.f, rec.d[].d3)

CREATE INDEX idx_b on foo (rec.b[])

create index idx_mod on foo(modification_time())


CREATE TABLE Boo(   
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER,
  address RECORD(   
          city STRING,
          state STRING,
          phones ARRAY( RECORD ( work INTEGER, home INTEGER ) )), 
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)


CREATE INDEX idx_anna_friends ON Boo (children.Anna.friends[])
