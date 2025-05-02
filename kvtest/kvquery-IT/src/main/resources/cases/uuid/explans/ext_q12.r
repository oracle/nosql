compiled-query-plan

{
"query file" : "uuid/q/ext_q12.q",
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
      "index used" : "idx_uid2",
      "covering index" : true,
      "index row variable" : "$$foo_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "uid2" : { "start value" : "00000000-0000-0000-0000-000000000000", "start inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$uuid1"
        }
      ],
      "map of key bind expressions" : [
        [ 0, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo_idx",
    "SELECT expressions" : [
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
