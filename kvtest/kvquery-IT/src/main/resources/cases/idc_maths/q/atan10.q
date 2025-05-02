#atan of maps and nested map and in json values
select t.numMap, atan(t.numMap) as atannumMap,
       t.numNestedMap, atan(t.numNestedMap) as atannumNestedMap,
       t.douMap, atan(t.douMap) as atandouMap,
       t.douNestedMap, atan(t.douNestedMap) as atandouNestedMap,
       t.doc.numMap as docnumMap, atan(t.doc.numMap) as atandocnumMap,
       t.doc.numNestedMap as docnumNestedMap, atan(t.doc.numNestedMap) as atandocnumNestedMap,
       t.doc.douMap as docdouMap, atan(t.doc.douMap) as atandocdouMap,
       t.doc.douNestedMap as docdouNestedMap, atan(t.doc.douNestedMap) as atandocdouNestedMap
 from functional_test t where id=1