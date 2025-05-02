#degrees of maps and nested map and in json values
select t.numMap, degrees(t.numMap) as degreesnumMap,
       t.numNestedMap, degrees(t.numNestedMap) as degreesnumNestedMap,
       t.douMap, degrees(t.douMap) as degreesdouMap,
       t.douNestedMap, degrees(t.douNestedMap) as degreesdouNestedMap,
       t.doc.numMap as docnumMap, degrees(t.doc.numMap) as degreesdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, degrees(t.doc.numNestedMap) as degreesdocnumNestedMap,
       t.doc.douMap as docdouMap, degrees(t.doc.douMap) as degreesdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, degrees(t.doc.douNestedMap) as degreesdocdouNestedMap
 from functional_test t where id=1