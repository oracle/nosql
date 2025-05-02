compiled-query-plan

{
"query file" : "idc_in_expr/q/q13.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "fooNew",
      "row variable" : "$$fooNew",
      "index used" : "idx_fooNew1234",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"foo1":7},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$fooNew",
    "SELECT expressions" : [
      {
        "field name" : "fooNew",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$fooNew"
        }
      }
    ]
  }
}
}