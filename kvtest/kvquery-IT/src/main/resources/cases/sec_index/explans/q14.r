compiled-query-plan

{
"query file" : "sec_index/q/q14.q",
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
      "target table" : "T1",
      "row variable" : "$$t1",
      "index used" : "idx1",
      "covering index" : true,
      "index row variable" : "$$t1_idx",
      "index scans" : [
        {
          "equality conditions" : {"name":"alex"},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t1_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 0
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$$t1_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t1_idx"
          }
        }
      },
      {
        "field name" : "name",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "name",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$t1_idx"
          }
        }
      }
    ]
  }
}
}