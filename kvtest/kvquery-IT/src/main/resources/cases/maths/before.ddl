CREATE TABLE IF NOT EXISTS math_test (
    id INTEGER,
    ic INTEGER,
    lc LONG,
    fc FLOAT,
    dc DOUBLE,
    nc NUMBER,
    numArr ARRAY(NUMBER),
    doubArr ARRAY(DOUBLE),
    doc JSON,
    PRIMARY KEY (id)
)

create index if not exists idx_dc on math_test(dc)

create index if not exists idx_floor_dc on math_test(floor(dc))

create index if not exists idx_power_ic on math_test(power(ic,2))

create index if not exists idx_round on math_test(round(fc))

create index if not exists idx_floor_jdc on math_test(floor(doc.dc as double))

create index if not exists idx_abs_array on math_test(abs(numArr[]))

create index if not exists idx_degree_array on math_test(degrees(doubArr[]))

create index if not exists idx_ln_jarray on math_test(ln(doc.numArr[] as number))

create index if not exists idx_trunc_jarray on math_test(trunc(doc.doubArr[] as double))

