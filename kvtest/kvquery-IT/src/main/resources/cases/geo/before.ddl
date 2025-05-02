
create table points (id integer, info json, primary key(id))


create index idx_ptn on points(info.point as point)

create index idx_kind_ptn on points(info.kind as string,
                                    info.point as point)

create index idx_kind_ptn_city on points(info.kind as string,
                                         info.point as point,
                                         info.city as string)




create table companies (id integer, locations json, primary key(id))

create index idx_loc on companies (locations[].loc as point)


create table companies2 (id integer, info json, primary key(id))

create index idx_kind_loc on companies2 (
    info.locations.features[].properties.kind as string,
    info.locations.features[].geometry as point)


create table polygons (id integer, info json, primary key(id))

create index idx_geom on polygons(info.geom as geometry {"max_covering_cells":400})

create index idx_city_geom on polygons(
    info.city as string,
    info.geom as geometry {"max_covering_cells":400})

create index idx_kind_city_geom on polygons(
    info.kind as string,
    info.city as string,
    info.geom as geometry {"max_covering_cells":400})


create table routes (id integer, info json, primary key(id))


create table temp (id integer, info json, primary key(id))
