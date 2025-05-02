#TestDescription: expression includes cast expression for source
#Expected result: return true

select regex_like(cast(p.info.id.stats2.odi[0] as string),".*1.*") from playerinfo p where id1=1