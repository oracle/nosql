#TestDescription: source is a string field of a json column

select id1 from playerinfo p where regex_like(p.info.id.name,"V.*")