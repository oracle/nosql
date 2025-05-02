#exp of maps and nested map and in json values
select t.numMap, exp(t.numMap) as expnumMap,
       t.numNestedMap, exp(t.numNestedMap) as expnumNestedMap,
       t.douMap, exp(t.douMap) as expdouMap,
       t.douNestedMap,exp(t.douNestedMap) as expdouNestedMap,
       t.doc.numMap as docnumMap, exp(t.doc.numMap) as expdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, exp(t.doc.numNestedMap) as expdocnumNestedMap,
       t.doc.douMap as docdouMap, exp(t.doc.douMap) as expdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, exp(t.doc.douNestedMap) as expdocdouNestedMap
 from functional_test t where id=1