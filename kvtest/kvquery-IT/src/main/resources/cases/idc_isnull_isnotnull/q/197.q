#Expression with not operator and is null in projection
select id, not s.children.values().Relative is  null,s.map.IDP  from sn s