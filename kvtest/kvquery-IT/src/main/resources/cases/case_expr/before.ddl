###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###

CREATE TABLE Foo (
  firstName STRING,
  lastName STRING,
  id INTEGER,
  age LONG,
  ptr STRING,
  double DOUBLE,
  float FLOAT,
  address RECORD( city STRING,
                  state STRING,
                  phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
                  ptr STRING),
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)


CREATE INDEX idx_state_city_age ON Foo (address.state, address.city, age, lastname)

CREATE INDEX idx_phones ON Foo (address.phones[].work, address.phones[].home)

CREATE INDEX idx_children ON Foo (children.keys(), children.values().age)
