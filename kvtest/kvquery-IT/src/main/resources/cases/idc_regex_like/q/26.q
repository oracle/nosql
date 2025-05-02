#TestDescription: source is a string element of a array column

select id, id1, name, age from playerinfo p where regex_like(cast(p.info.id.stats1[0] as string),".*1.*") and id=100 and id1=1
