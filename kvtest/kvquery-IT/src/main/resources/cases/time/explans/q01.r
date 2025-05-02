compiled-query-plan

{
"query file" : "time/q/q01.q",
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
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"time1":"2014-05-05T10:45:00.234Z"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$Foo",
    "SELECT expressions" : [
      {
        "field name" : "id1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
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
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "time2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "time2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$Foo"
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "CONST",
          "value" : "2015-01-01T10:45:00.234000000Z"
        }
      }
    ]
  }
}
}