# Test for regex_like function With Group by clause

SELECT count(id1) as count, Country FROM playerinfo p where regex_like(p.profile,".*s.*","i")  GROUP BY Country