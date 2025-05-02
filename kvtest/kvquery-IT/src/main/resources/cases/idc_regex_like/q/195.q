# Test for Classes for Unicode scripts, blocks, categories and binary properties
# should return error

SELECT id1 FROM playerinfo p where regex_like(p.profile,"\\p{Lu}")