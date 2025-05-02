#Expression returns non json map with is null in projection and exists in predicate
select id, s.map.IDP[].Passport is null from sn s where exists  s.map.IDP[].DL