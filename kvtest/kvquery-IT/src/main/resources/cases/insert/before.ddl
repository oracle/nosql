
create table foo(
   str string default "abc",
   id1 integer,
   id2 integer,
   info json,
   rec1 record(long long,
               rec2 record(str string,
                           num number default 10,
                           arr array(integer)),
               recinfo json,
               map map(string)
        ),
   primary key(shard(id1), id2)
) using ttl 5 days


create table t1(c1 integer,
                c2 integer not null default 100,
                ident integer GENERATED ALWAYS AS IDENTITY (START WITH 1),
                t timestamp(3),
                primary key(c1))


create table tIdentity(
  id integer,
  id1 long generated always as identity,
  name string,
  primary key(shard(id), id1)
)
