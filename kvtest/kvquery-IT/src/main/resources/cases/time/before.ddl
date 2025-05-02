create table foo (
    id1 integer,
    id2 timestamp(0),
    id3 string,
    age integer,
    bool boolean,
    time1 timestamp(3),
    time2 timestamp(9),
    primary key (id1, id2, id3)
)

create index idx_age_ts1_name on foo (age, time1, id3)

create index idx_ts1_name on foo (time1, id3)
    
create index idx_name_ts1_bool on foo (id3, time1, bool)

create index idx_year2_name on foo (year(time2), id3)

create index idx_modtime on foo (modification_time())

create index idx_upper on foo (upper(time2))


create table bar (
    id integer,
    tm timestamp(3),
    primary key(id))

create index idx_tm on bar (tm)

create index idx_year_month on bar (year(tm), month(tm))

create index idx_year_hour on bar (year(tm), hour(tm))

create table bar2 (
    id integer,
    tm timestamp(0),
    tmarr ARRAY(timestamp(3)),
    starr ARRAY(string),
    age integer,
    income integer,
    map map(string),
    primary key(id))

create index idx_tm on bar2 (tm)

create index idx_tmarr on bar2 (tmarr[])

create index idx_starr on bar2 (substring(starr[], 0, 4))

create index idx_age_tm on bar2 (age, tm)

create index idx_income on bar2 (income)

create index idx_year_month on bar2 (year(tmarr[]), month(tmarr[]))

create index idx_tm_month on bar2 (tmarr[], month(tmarr[]))

create index idx_map_year_month on bar2 (substring(map.values(), 0, 4),
                                         substring(map.values(), 5, 2))

create index idx_map_keys_year_month on bar2 (map.keys(),
                                              substring(map.values(), 0, 4),
                                              substring(map.values(), 5, 2))

create index idx_map_keys_values_year on bar2(map.keys(),
                                              map.values(),
                                              substring(map.values(), 0, 4))

create index idx_map_upper_keys on bar2(upper(map.keys()))

create table arithtest(
    id integer,
    tm0 timestamp(0),
    tm3 timestamp(3),
    tm9 timestamp(9),
    duration string,
    info json,
    primary key(id)
)

create table foo2(id integer, json json, arr array(String), primary key(id))

create index idx_json on foo2(json[] as string)

#Table roundtest
CREATE TABLE IF NOT EXISTS roundtest (
    id INTEGER,
    t0 TIMESTAMP(0),
    t3 TIMESTAMP(3),
    s9 STRING,
    l3 LONG,
    doc JSON,
    PRIMARY KEY(id)
)

CREATE INDEX IF NOT EXISTS idx_t3_last_day on roundtest(to_last_day_of_month(t3))

CREATE INDEX IF NOT EXISTS idx_js6_last_day_jl3_year on roundtest(to_last_day_of_month(doc.s6 as string), year(doc.l3 as long))

CREATE INDEX IF NOT EXISTS idx_ceil_t0 on roundtest(timestamp_ceil(t0))

CREATE INDEX IF NOT EXISTS idx_ceil_t0_month on roundtest(timestamp_ceil(t0, 'month'))

CREATE INDEX IF NOT EXISTS idx_floor_js6 on roundtest(timestamp_floor(doc.s6 as STRING))

CREATE INDEX IF NOT EXISTS idx_floor_js6_year on roundtest(timestamp_floor(doc.s6 as STRING, 'year'))

CREATE INDEX IF NOT EXISTS idx_trunc_s9 on roundtest(timestamp_trunc(s9))

CREATE INDEX IF NOT EXISTS idx_trunc_s9_day on roundtest(timestamp_trunc(s9, 'day'))

CREATE INDEX IF NOT EXISTS idx_round_arr on roundtest(timestamp_round(doc.arr[] as STRING))

CREATE INDEX IF NOT EXISTS idx_round_arr_year on roundtest(timestamp_round(doc.arr[] as STRING, 'year'))

CREATE INDEX IF NOT EXISTS idx_bucket_t3 on roundtest(timestamp_bucket(t3, '5 minutes'))

CREATE INDEX IF NOT EXISTS idx_trunc_jl3 on roundtest(timestamp_trunc(doc.l3 as long, 'hour'), timestamp_trunc(l3))

