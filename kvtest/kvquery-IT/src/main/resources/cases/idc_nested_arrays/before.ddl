#  target nested arrays


create table nestedTable (
     Id integer,
     firstName string,
     lastName string,
     age integer,
     addresses array(record(
             city string,
             state string,
             phones array(array(record(
                  areacode integer,
                  number integer,
                  kind string)))
             )),
     array1 array(array(record(
          a integer ))),
     array2 array(array(integer)),
     maps array(map(record(
        foo string,
        bar integer,
        array array(array(integer))
     ))),
     primary key(id)
)

create index idx_age_areacode_kind on nestedTable ( 
     age, 
     addresses[].phones[][].areacode,
     addresses[].phones[][].kind)

create index idx_age_array on nestedTable ( 
     age, 
     maps[].values().array[][])

create index idx_array_foo_bar on nestedTable (
     maps[].values().array[][], 
     maps[].values().foo,
     maps[].values().bar)

create index idx_city_state_areacode on nestedTable ( 
     addresses[].city, 
     addresses[].state, 
     addresses[].phones[][].areacode)

create index idx_firstName_number_kind on nestedTable ( 
     firstName, 
     addresses[].phones[][].number, 
     addresses[].phones[][].kind)

create index idx_foo_bar on nestedTable ( 
     maps[].values().foo, 
     maps[].values().bar)

