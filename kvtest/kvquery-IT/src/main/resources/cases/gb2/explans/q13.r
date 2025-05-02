compiled-query-plan

{
"query file" : "gb2/q/q13.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "T1",
        "row variable" : "$$t",
        "index used" : "idx_json_ALON",
        "covering index" : false,
        "index row variable" : "$$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#ID",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 10023
          }
        },
        "position in join" : 0
      },
      "FROM variable" : "$$t",
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "ALON",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ALON",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "AJSO",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          }
        },
        {
          "field name" : "Column_2",
          "field expression" : 
          {
            "iterator kind" : "FN_MAX",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "BRRY",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "AREC",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "ADD_SUBTRACT",
        "operations and operands" : [
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          },
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 10
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "ALON",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ALON",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "FN_MAX",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Column_2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 10
  }
}
}