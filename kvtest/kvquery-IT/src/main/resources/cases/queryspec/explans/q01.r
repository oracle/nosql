compiled-query-plan

{
"query file" : "queryspec/q/q01.q",
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
      "target table" : "Users2",
      "row variable" : "$$u",
      "index used" : "idx2",
      "covering index" : true,
      "index row variable" : "$$u_idx",
      "index scans" : [
        {
          "equality conditions" : {"address.state":"CA"},
          "range conditions" : { "address.city" : { "start value" : "S", "start inclusive" : true } }
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "CONST",
              "value" : 10
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "income",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$u_idx"
              }
            }
          },
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "income",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$u_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 20
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variable" : "$$u_idx",
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
            "variable" : "$$u_idx"
          }
        }
      },
      {
        "field name" : "income",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "income",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$u_idx"
          }
        }
      }
    ]
  }
}
}