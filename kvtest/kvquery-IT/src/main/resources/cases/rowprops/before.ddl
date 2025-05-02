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
  ptr STRING,       
  address RECORD(   
          city STRING,
          state STRING,
          phones ARRAY( RECORD ( work INTEGER, home INTEGER ) ),
          ptr STRING), 
  children MAP( RECORD( age LONG, iage INTEGER, friends ARRAY(STRING) ) ),
  primary key (id)
) using ttl 5 days


CREATE INDEX idx_state_city_age ON Foo (address.state, address.city, age)

CREATE INDEX idx_children_anna on Foo (children.Anna.iage)

create index idx_city_phones on foo (address.city, address.phones[].work)


create table t_user1(id INTEGER, primary key(id))

create index idx_t_user1 on t_user1 (id)


CREATE TABLE Boo(id INTEGER, primary key (id)) as json collection using ttl 5 days

CREATE INDEX idx_state_city_age ON Boo (
    address.state as string,
    address.city as string,
    age as integer)

CREATE INDEX idx_children_anna on Boo (children.Anna.iage as integer)

create index idx_city_phones on Boo (
    address.city as string,
    address.phones[].work as integer)

create index idx_mod_time on Boo (modification_time())
