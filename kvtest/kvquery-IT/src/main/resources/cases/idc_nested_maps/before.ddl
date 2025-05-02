#  Statements must be delimited by at least an empty line. Statementes can
#  use multiple consecutive lines.
#  target nested maps

create table nestedTable (
     id integer,
     firstName string,
     lastName string,
     age integer,
     addresses map(record(
                            city string,
                            state string,
                            phones map(map(record(
                                      areacode integer,
                                      number integer,
                                      kind string)))
    )),
    map1 map(record(
                    foo string,
                    bar string,
                    map2 map(integer)
                    )),
   Primary key(id)
)

create index idx_age_areacode_kind on nestedTable (
      age, 
      addresses.values().phones.values().values().areacode,
      addresses.values().phones.values().values().kind)

create index idx_areacode_number on nestedTable (
     addresses.values().phones.values().values().areacode, 
     addresses.values().phones.values().values().number)

create index idx_map1_keys_map2_values on nestedTable (
     map1.keys(), 
     map1.values().map2.values())

create index idx_keys_number_kind on nestedTable (
     addresses.values().phones.values().keys(),
     addresses.values().phones.values().values().number, 
     addresses.values().phones.values().values().kind)


