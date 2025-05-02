###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(   
  id INTEGER,
  arr ARRAY(INTEGER),
  record RECORD(long LONG, int INTEGER, string STRING, bool BOOLEAN, float FLOAT),
  info JSON,
  primary key (id)
)

