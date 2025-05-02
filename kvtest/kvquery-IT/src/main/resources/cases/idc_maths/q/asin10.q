#asin of maps and nested map and in json values
select t.numMap, asin(t.numMap) as asinnumMap,
       t.numNestedMap, asin(t.numNestedMap) as asinnumNestedMap,
       t.douMap, asin(t.douMap) as asindouMap,
       t.douNestedMap, asin(t.douNestedMap) as asindouNestedMap,
       t.doc.numMap as docnumMap, asin(t.doc.numMap) as asindocnumMap,
       t.doc.numNestedMap as docnumNestedMap, asin(t.doc.numNestedMap) as asindocnumNestedMap,
       t.doc.douMap as docdouMap, asin(t.doc.douMap) as asindocdouMap,
       t.doc.douNestedMap as docdouNestedMap, asin(t.doc.douNestedMap) as asindocdouNestedMap
 from functional_test t where id=1