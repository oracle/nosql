# Test for Greedy quantifiers
# should return error

SELECT id1 FROM playerinfo p where regex_like(p.profile,"X{10,12}")