#Expression returns Json Atomic Int with is not null in projection
select id, s.children.George.age is not null from sn s