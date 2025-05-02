#Expression is array slice step (with L and H) with L<=H in projection and .values() with is not null in projection 
select id,s.children.values().Relative.Uncle[1:2] is not null from sn s