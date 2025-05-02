#acos of maps and nested map and in json values
select t.numMap, acos(t.numMap) as acosnumMap,
       t.numNestedMap, acos(t.numNestedMap) as acosnumNestedMap,
       t.douMap, acos(t.douMap) as acosdouMap,
       t.douNestedMap, acos(t.douNestedMap) as acosdouNestedMap,
       t.doc.numMap as docnumMap, acos(t.doc.numMap) as acosdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, acos(t.doc.numNestedMap) as acosdocnumNestedMap,
       t.doc.douMap as docdouMap, acos(t.doc.douMap) as acosdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, acos(t.doc.douNestedMap) as acosdocdouNestedMap
 from functional_test t where id=1