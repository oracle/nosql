
create table foo (
 id integer,
 foo1 integer,
 foo2 double,
 foo3 string,
 foo4 long,
 info json,
 primary key (id)
)


create index idx_foo1234 on foo (foo1, foo2, foo3, foo4)

create index idx_bar1234 on foo (info.bar1 as integer,
                                 info.bar2 as double,
                                 info.bar3 as string,
                                 info.bar4 as long)


create index idx_phones on foo (info.phones[].num as integer,
                                info.phones[].kind as string)
