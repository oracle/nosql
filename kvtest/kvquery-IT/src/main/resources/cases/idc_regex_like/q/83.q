#TestDescription: Finds any values that start with "a" and are at least 3 characters in length

select id1,p.info.id.name from playerinfo p where regex_like(p.info.id.name,"a...*","i")
