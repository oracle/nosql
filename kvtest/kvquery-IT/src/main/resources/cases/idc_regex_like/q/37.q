#TestDescription: source is of type json null
#Expected result: return false

select regex_like(p.info.id,"null") from playerinfo p where id1=2