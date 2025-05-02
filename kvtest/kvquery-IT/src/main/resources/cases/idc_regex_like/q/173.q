# Test for regex_like function With order by clause

SELECT id1, Country FROM playerinfo p where regex_like(p.profile,".*s.*","i") order by country