###
#  DDL comands executed when test.config contains property:
#     before-ddl-file   = file-name.ddl
#
#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
###
CREATE TABLE Foo(   
  id INTEGER,
  firstName STRING,
  lastName STRING,
  age INTEGER,
  address RECORD(   
          city STRING,
          state STRING,
          phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
          ptr STRING), 
  children MAP( RECORD( age LONG, friends ARRAY(STRING) ) ),
  primary key (id)
)

CREATE INDEX idx_state_city_age ON Foo (address.state, address.city, age)

CREATE INDEX idx_children on Foo (children.keys(), children.values().age)

CREATE INDEX idx_children_anna on Foo (children.Anna.age)

CREATE INDEX idx_children_jlf on Foo (children.John.age, 
                                      children.Lisa.age,
                                      children.Fred.age)

create index idx_phones on foo (address.phones[].work, 
                                address.state, 
                                address.phones[].home)
