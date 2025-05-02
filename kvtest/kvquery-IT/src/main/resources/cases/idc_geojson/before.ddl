
create table points (id integer, info json, primary key(id))


create index idx_ptn on points(info.point as point)

create index idx_kind_ptn on points(info.kind as string,
                                    info.point as point)

create index idx_kind_ptn_city on points(info.kind as string,
                                         info.point as point,
                                         info.city as string)

create table geotypes(id integer, info json, primary key(id))

create table testsqlnull(id integer, info json, info1 json, primary key(id))

create table geosrs(pk integer, geojson json, primary key(pk))