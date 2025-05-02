#Expression with not operator and is not null in projection
select id, not s.children.values().Relative is not null,s.map.IDP  from sn s