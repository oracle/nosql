#Test Description: regex_like function in predicate. Regex pattern that causes Pattern.Matcher.matches() to get StackOverFlowError
## Type: Negative

select id1, str
from foo f
where regex_like(longstr, "(a|aa)+")
