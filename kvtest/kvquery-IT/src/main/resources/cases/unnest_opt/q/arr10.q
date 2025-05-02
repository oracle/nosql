select $areakind.areacode, $areakind.kind, avg(age) as age
from foo as $t, seq_distinct(
                seq_transform($t.address.phones[],
                              { "areacode" : $.areacode,
                                "kind" : $.kind })) as $areakind
group by $areakind.areacode, $areakind.kind
