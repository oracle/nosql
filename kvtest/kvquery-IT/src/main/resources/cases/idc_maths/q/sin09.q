#sin of maps and nested map and in json values
select t.numMap, sin(t.numMap) as sinnumMap,
       t.numNestedMap, sin(t.numNestedMap) as sinnumNestedMap,
       t.douMap, sin(t.douMap) as sindouMap,
       t.douNestedMap, sin(t.douNestedMap) as sindouNestedMap,
       t.doc.numMap as docnumMap, sin(t.doc.numMap) as sindocnumMap,
       t.doc.numNestedMap as docnumNestedMap, sin(t.doc.numNestedMap) as sindocnumNestedMap,
       t.doc.douMap as docdouMap, sin(t.doc.douMap) as sindocdouMap,
       t.doc.douNestedMap as docdouNestedMap, sin(t.doc.douNestedMap) as sindocdouNestedMap
 from functional_test t where id=1