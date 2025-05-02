#Table roundFunc
CREATE TABLE IF NOT EXISTS roundFunc (
    id INTEGER,
    t0 TIMESTAMP(0),
    t3 TIMESTAMP(3),
    s9 STRING,
    s3 STRING,
    l3 LONG,
    doc JSON,
    PRIMARY KEY(id)
)

CREATE TABLE IF NOT EXISTS jsonCollection_test(
    id INTEGER,
    PRIMARY KEY (id)
) as JSON collection


CREATE INDEX IF NOT EXISTS idx_ceil_t0 on roundFunc(timestamp_ceil(t0))

CREATE INDEX IF NOT EXISTS idx_ceil_t0_quarter on roundFunc(timestamp_ceil(t0, 'quarter'))

CREATE INDEX IF NOT EXISTS idx_ceil_arr_year on roundFunc(timestamp_ceil(doc.arr[] as STRING, 'year'))

CREATE INDEX IF NOT EXISTS idx_ceil_map_month on roundFunc(timestamp_ceil(doc.map.keys(), 'month'))

CREATE INDEX IF NOT EXISTS idx_floor_t3 on roundFunc(timestamp_floor(t3))

CREATE INDEX IF NOT EXISTS idx_floor_t3_hour on roundFunc(timestamp_floor(t3, 'hour'))

CREATE INDEX IF NOT EXISTS idx_floor_arr_month on roundFunc(timestamp_floor(doc.arr[] as STRING, 'month'))

CREATE INDEX IF NOT EXISTS idx_floor_map_minute on roundFunc(timestamp_floor(doc.map.keys(), 'minute'))

CREATE INDEX IF NOT EXISTS idx_round_t0 on roundFunc(timestamp_round(t0))

CREATE INDEX IF NOT EXISTS idx_round_t0_year on roundFunc(timestamp_round(t0, 'year'))

CREATE INDEX IF NOT EXISTS idx_round_arr_iyear on roundFunc(timestamp_round(doc.arr[] as STRING, 'iyear'))

CREATE INDEX IF NOT EXISTS idx_round_map_iweek on roundFunc(timestamp_round(doc.map.keys(), 'iweek'))

CREATE INDEX IF NOT EXISTS idx_trunc_t3 on roundFunc(timestamp_trunc(t3))

CREATE INDEX IF NOT EXISTS idx_bucket_t3_7_days on roundFunc(timestamp_bucket(t3, '7 days'))

CREATE INDEX IF NOT EXISTS idx_trunc_arr_second on roundFunc(timestamp_trunc(doc.arr[] as STRING, 'second'))

CREATE INDEX IF NOT EXISTS idx_trunc_map_day on roundFunc(timestamp_trunc(doc.map.keys(), 'day'))

CREATE INDEX IF NOT EXISTS idx_to_last_day_of_month_json on roundFunc(to_last_day_of_month(doc.s6 as STRING))

CREATE INDEX IF NOT EXISTS idx_quarter_l3 on roundFunc(quarter(l3))

CREATE INDEX IF NOT EXISTS idx_day_of_week_arr on roundFunc(day_of_week(doc.arr[] as STRING))

CREATE INDEX IF NOT EXISTS idx_day_of_month_arr on roundFunc(day_of_month(doc.map.keys()))

CREATE INDEX IF NOT EXISTS idx_day_of_year_json on jsonCollection_test(day_of_year(t.str4 as STRING))