#ceil of maps and nested map and in json values
select t.numMap, ceil(t.numMap) as ceilnumMap,
       t.numNestedMap, ceil(t.numNestedMap) as ceilnumNestedMap,
       t.douMap, ceil(t.douMap) as ceildouMap,
       t.douNestedMap, ceil(t.douNestedMap) as ceildouNestedMap,
       t.doc.numMap as docnumMap, ceil(t.doc.numMap) as ceildocnumMap,
       t.doc.numNestedMap as docnumNestedMap, ceil(t.doc.numNestedMap) as ceildocnumNestedMap,
       t.doc.douMap as docdouMap, ceil(t.doc.douMap) as ceildocdouMap,
       t.doc.douNestedMap as docdouNestedMap, ceil(t.doc.douNestedMap) as ceildocdouNestedMap
 from functional_test t where id=1