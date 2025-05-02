compiled-query-plan

{
"query file" : "queryspec/q/q02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
    "input iterator" :
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
          "covering index" : false,
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
        "FROM variable" : "$$u",
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$u"
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
                "variable" : "$$u"
              }
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "FN_SEQ_SUM",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "expenses",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$u"
                  }
                }
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
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
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}