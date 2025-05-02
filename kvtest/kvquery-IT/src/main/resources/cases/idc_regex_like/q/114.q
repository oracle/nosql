#TestDescription: flag is sql null value
#Expected result: return NULL

select regex_like("vinay","vinay",p.name) from playerinfo p where id1=2