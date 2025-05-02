#expression returns year. The Expression is a map filter step with .keys() used with Extract function on non json map
#Negaive Case 
SELECT id,extract(year from cast(t.json.keys($keys="k1") as timestamp)) FROM Extract t
