# Test for java.lang.Character classes (simple java character type)
# should return error

SELECT id1 FROM playerinfo p where regex_like(p.profile,"\\p{javaWhitespace}")