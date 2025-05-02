#Expression returns non json map with is null in predicate
select id,map from sn s where s.map.IDP[].DL is null