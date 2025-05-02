compiled-query-plan

{
"query file" : "prim_index/q/q12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "id1" : { "end value" : 2, "end inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "SELECT expressions" : [
      {
        "field name" : "foo",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$foo"
        }
      }
    ]
  }
}
}