create table foo (
    id integer,
    name string,
    age integer,
    time9 timestamp(9),
    primary key (id, name)
) USING TTL 1 HOURS

create index idx_modtime on foo (modification_time())

create index idx_exptime on foo (expiration_time())

create index idx_exptime_ms on foo (expiration_time_millis())

create index idx_row_size on foo (row_storage_size())

create index idx_length_name on foo (length(name))

create index idx_replace_name on foo (replace(name, 'A', 'XXX'))

create index idx_reverse_name on foo (reverse(name))

create index idx_upper_name on foo (upper(name))

create index idx_lower_name on foo (lower(name))

create index idx_trim_name on foo (trim(name))

create index idx_trim_name_leading on foo (trim(name, "leading"))

create index idx_trim_name_trailing on foo (trim(name, "trailing"))

create index idx_trim_name_leading_A on foo (trim(name, "leading", "A"))

create index idx_trim_name_trailing_z on foo (trim(name, "trailing", "z"))

create index idx_ltrim_name on foo (ltrim(name))

create index idx_rtrim_name on foo (rtrim(name))

create index idx_substring_name_pos_len on foo (substring(name, 4, 3))

create table bar (
    id integer,
    tm timestamp(9),
    primary key(id))

create index idx_year_month_day on bar (year(tm), month(tm), day(tm))

create index idx_hour_min_sec_ms_micro_ns on bar (hour(tm), minute(tm), second(tm), millisecond(tm), microsecond(tm), nanosecond(tm))

create index idx_week on bar (week(tm))

create table bar2 (
    id integer,
    tm timestamp(0),
    tmarr ARRAY(timestamp(3)),
    starr ARRAY(string),
    age integer,
    income integer,
    primary key(id))

create index idx_starr_year on bar2 (substring(starr[], 0, 4))

create index idx_starr on bar2 (substring(starr[], 0, 4), substring(starr[], 5, 2))

create index idx_year_month on bar2 (year(tmarr[]), month(tmarr[]))
