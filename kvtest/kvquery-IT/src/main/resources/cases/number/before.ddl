CREATE TABLE NumTable(
    id   NUMBER,
    num  NUMBER,
    json JSON,
    map  MAP(ARRAY(NUMBER)),
    rec  RECORD(fmap MAP(ARRAY(NUMBER))),
    arrrec ARRAY(RECORD(fmap MAP(ARRAY(NUMBER)))),
    primary key (id)
)

CREATE INDEX idx_num1 ON NumTable(num)

