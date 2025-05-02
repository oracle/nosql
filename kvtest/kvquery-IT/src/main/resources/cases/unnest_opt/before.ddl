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
          phones ARRAY( RECORD( areacode INTEGER, 
                                number INTEGER,
                                kind ENUM(work, home) 
                              ) 
                       )
          ), 
  children MAP( RECORD( age LONG, school STRING, friends ARRAY(STRING) ) ),
  primary key (id)
)


CREATE INDEX idx_state_city_age ON Foo (address.state, address.city, age)

CREATE INDEX idx_phones ON Foo (address.state, 
                                address.phones[].areacode,
                                address.phones[].kind)

CREATE INDEX idx_phones2 ON Foo (address.phones[].areacode,
                                 address.phones[].kind)

CREATE INDEX idx_children_both ON Foo (children.keys(),
                                       children.values().age,
                                       children.values().school)


create table User ( 
     Id integer, 
     firstName string, 
     lastName string, 
     age integer, 
     addresses array(record(city string, 
                            state string, 
                            phones array(array(record(areacode integer, 
                                                      number integer, 
                                                      kind string))) 
                           )
                    ),
    children map(record( age long, school string, friends array(string))), 
    primary key (id)
)


create INDEX idx_state_city_age ON User(
     addresses[].state,
     addresses[].city,
     age)
with unique keys per row 

create INDEX idx_phones ON User(
    addresses[].state, 
    addresses[].phones[][].areacode, 
    addresses[].phones[][].kind)
with unique keys per row 

create INDEX idx_phones2 ON User(
    addresses[].phones[][].areacode, 
    addresses[].phones[][].kind)
with unique keys per row 

create INDEX idx_children_both ON User(
    children.keys(), 
    children.values().friends[]) 
with unique keys per row 


