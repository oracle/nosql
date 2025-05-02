#Expression is array slice step (with L and H) with L>H in projection with is not null in projection
select id,s.children.Fred.Relative.Uncle[1:0] is not null from sn s