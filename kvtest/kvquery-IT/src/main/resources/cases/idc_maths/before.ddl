CREATE TABLE IF NOT EXISTS functional_test (
    id INTEGER,
    iv INTEGER,
    lv LONG,
    fv FLOAT,
    dv DOUBLE,
    nv NUMBER,
    numArr ARRAY(NUMBER),
    num2DArr ARRAY(ARRAY(NUMBER)),
    num3DArr ARRAY(ARRAY(ARRAY(NUMBER))),
    douArr ARRAY(DOUBLE),
    dou2DArr ARRAY(ARRAY(DOUBLE)),
    dou3DArr ARRAY(ARRAY(ARRAY(DOUBLE))),
    numMap MAP(NUMBER),
    numNestedMap MAP(MAP(NUMBER)),
    douMap MAP(DOUBLE),
    douNestedMap MAP(MAP(DOUBLE)),
    nestedNumMapInArray ARRAY(MAP(NUMBER)),
    nestedDouMapInArray ARRAY(MAP(DOUBLE)),
    nestedNumArrayInMap Map(ARRAY(NUMBER)),
    nestedDouArrayInMap MAP(ARRAY(DOUBLE)),
    doc JSON,
    PRIMARY KEY (id)
)

CREATE TABLE IF NOT EXISTS jsonCollection_test(
    id INTEGER,
    PRIMARY KEY (id)
) as JSON collection

CREATE TABLE IF NOT EXISTS aggregate_test(
    id INTEGER,
    value1 DOUBLE,
    value2 DOUBLE,
    value3 DOUBLE,
    PRIMARY KEY (id)
)

create index if not exists idx_nv on functional_test(nv)

create index if not exists idx_ceil_dv on functional_test(ceil(dv))

create index if not exists idx_ceil_array on functional_test(ceil(douArr[]))

create index if not exists idx_ceil_map on functional_test(ceil(douMap.values()))

create index if not exists idx_ceil_jsonarray on functional_test(ceil(doc.numArr[] as double))

create index if not exists idx_sin_fv on functional_test(sin(fv))

create index if not exists idx_sin_array on functional_test(sin(numArr[]))

create index if not exists idx_sin_map on functional_test(sin(numMap.values()))

create index if not exists idx_sin_jsonarray on functional_test(sin(doc.douArr[] as double))

create index if not exists idx_radians_nv on functional_test(radians(nv))

create index if not exists idx_radians_array on functional_test(radians(numArr[]))

create index if not exists idx_radians_map on functional_test(radians(douMap.values()))

create index if not exists idx_radians_jsonarray on functional_test(radians(doc.douArr[] as double))

create index if not exists idx_sqrt_iv on functional_test(sqrt(iv))

create index if not exists idx_sqrt_array on functional_test(sqrt(numArr[]))

create index if not exists idx_sqrt_jsonarray on functional_test(sqrt(doc.douArr[] as double))

create index if not exists idx_logten_lv on functional_test(log10(lv))

create index if not exists idx_logten_array on functional_test(log10(numArr[]))

create index if not exists idx_logten_jsonarray on functional_test(log10(doc.douArr[] as double))

create index if not exists idx_atan2_dv on functional_test(atan2(dv,3))

create index if not exists idx_acos_array on functional_test(acos(numArr[]))

create index if not exists idx_acos_jsonarray on functional_test(acos(doc.douArr[] as double))