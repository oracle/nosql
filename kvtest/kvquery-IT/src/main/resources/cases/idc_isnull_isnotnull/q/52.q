#Expression returns non json map with is null in predicate and exists(non empty) operator in projection
select id, exists s.map.IDP[].Passport from sn s where s.map.IDP[].DL is null