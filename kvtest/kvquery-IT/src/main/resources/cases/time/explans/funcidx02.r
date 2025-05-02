compiled-query-plan

{
"query file" : "time/q/funcidx02.q",
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
      "index used" : "idx_year2_name",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"year#time2":2015},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "time2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 1
      }
    },
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
            "variable" : "$$foo"
          }
        }
      }
    ]
  }
}
}