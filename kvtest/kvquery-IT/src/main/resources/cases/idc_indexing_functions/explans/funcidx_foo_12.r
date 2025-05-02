compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_foo_12.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "id" : 99,
  "age" : 57,
  "time9" : "2015-01-01T10:45:00.010234500Z"
},
  "column positions" : [ 1 ],
  "value iterators" : [
    {
      "iterator kind" : "EXTERNAL_VAR_REF",
      "variable" : "$name1"
    }
  ]
}
}