#Expression returns non json map with .values() and  is not null in projection with exists in predicate
select id, s.map.IDP[].Passport.values() is not null from sn s where exists s.map.IDP[].DL