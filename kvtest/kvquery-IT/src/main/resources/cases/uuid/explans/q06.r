compiled-query-plan

{
"query file" : "uuid/q/q06.q",
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
          "range conditions" : { "uid2" : { "end value" : "28acbcb9-137b-4fc8-99f7-812f20240358", "end inclusive" : false } }
        }
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