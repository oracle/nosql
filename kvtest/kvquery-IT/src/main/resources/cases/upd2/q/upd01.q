update playerinfo p
 SET p.name = "Rory Burns",
 REMOVE p.info.id.name
 where id=100 and id1=28
returning *
