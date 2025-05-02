compiled-query-plan

{
"query file" : "uuid/q/q18.q",
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
      "target table" : "foo",
      "row variable" : "$$foo",
      "index used" : "idx_int_uid2",
      "covering index" : true,
      "index row variable" : "$$foo_idx",
      "index scans" : [
        {
          "equality conditions" : {"int":3},
          "range conditions" : { "uid2" : { "start value" : "ffffffff-ffff-ffff-ffff-ffffffffffff", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo_idx",
    "SELECT expressions" : [
      {
        "field name" : "int",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "int",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo_idx"
          }
        }
      },
      {
        "field name" : "uid2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "uid2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo_idx"
          }
        }
      }
    ]
  }
}
}