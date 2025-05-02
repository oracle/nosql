#Test Description: expression includes map field step for source
#Expected result: return true

select id,id1,age,avg from playerinfo p where regex_like(p.info.id.name,"virat.*", "i")