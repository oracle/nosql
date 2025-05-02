#expression returns year. The Expression is a map filter step with .keys() used with Extract function on non json map
#Negaive Case 
SELECT id,extract(year from t.mts9.keys()) as mts4 FROM Extract t
