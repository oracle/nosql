compiled-query-plan
{
"query file" : "idc_indexing_functions/q/funcidx_foo_01.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "id" : 17,
  "name" : "name_17",
  "age" : 79,
  "time9" : "2015-01-01T10:45:00.010234500Z"
},
  "value iterators" : [

  ],
  "TTL iterator" :
  {
    "iterator kind" : "CONST",
    "value" : 5
  }
}
}
