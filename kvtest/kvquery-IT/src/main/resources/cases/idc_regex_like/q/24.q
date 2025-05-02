#TestDescription: expression return empty result for source
#Expected result: return error

select regex_like(cast(p.info.id.stats2.odi[0] as string),".*1.*") from playerinfo p