select $areakind.areacode, $areakind.kind, avg(age) as age
from User as $u, seq_distinct(
                seq_transform($u.addresses.phones[],
                              { "areacode" : $.areacode,
                                "kind" : $.kind })) as $areakind
group by $areakind.areacode, $areakind.kind
