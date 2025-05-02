#Expression with not operator and multiple is null and is not null in projection
select id, not s.children.values().Relative is not null,s.map.IDP is null from sn s