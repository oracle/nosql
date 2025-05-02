compiled-query-plan

{
"query file" : "time/q/q07.q",
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
      "row variable" : "$$Foo",
      "index used" : "idx_ts1_name",
      "covering index" : true,
      "index row variable" : "$$Foo_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "time1" : { "start value" : "2014-05-05T10:00:00.000Z", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$Foo_idx",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo_idx"
          }
        }
      },
      {
        "field name" : "time1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "time1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo_idx"
          }
        }
      }
    ]
  }
}
}