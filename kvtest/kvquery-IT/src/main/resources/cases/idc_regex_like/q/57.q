#TestDescription: pattern is json null value
#Expected result: return error

select regex_like("vinay",p.info.id.name) from playerinfo p where id1=2