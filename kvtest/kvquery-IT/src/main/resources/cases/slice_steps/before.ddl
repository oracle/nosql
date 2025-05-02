###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE Complex(
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER not null default 10,
  ptr STRING,
  address RECORD( city STRING,
                  state STRING,
                  phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                  ptr STRING),
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)
