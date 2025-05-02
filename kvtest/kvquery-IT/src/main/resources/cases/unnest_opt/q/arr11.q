select $areakind[0] as areacode, $areakind[1] as kind, avg(age) as age
from foo as $t, seq_distinct(
                seq_transform($t.address.phones[],
                              [ $.areacode, $.kind ] )) as $areakind
group by $areakind[0], $areakind[1]
