#TestDescription: pattern string is not valid.
#Expected result: return error

select regex_like("odi","a.ii.**O") from playerinfo p