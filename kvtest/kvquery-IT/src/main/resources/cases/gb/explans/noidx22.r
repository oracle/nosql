compiled-query-plan

{
"query file" : "gb/q/noidx22.q",
"plan" : 
{
  "iterator kind" : "GROUP",
  "input variable" : "$gb-2",
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "input iterator" :
    {
      "iterator kind" : "GROUP",
      "input variable" : "$gb-1",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "numbers",
          "row variable" : "$$numbers",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$numbers",
        "SELECT expressions" : [
          {
            "field name" : "number",
            "field expression" : 
            {
              "iterator kind" : "CASE",
              "clauses" : [
                {
                  "when iterator" :
                  {
                    "iterator kind" : "LESS_THAN",
                    "left operand" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "id",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$numbers"
                      }
                    },
                    "right operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 16
                    }
                  },
                  "then iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "number",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$numbers"
                    }
                  }
                },
                {
                  "else iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "decimal",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$numbers"
                    }
                  }
                }
              ]
            }
          },
          {
            "field name" : "cnt",
            "field expression" : 
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          }
        ]
      },
      "grouping expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "number",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$gb-1"
          }
        }
      ],
      "aggregate functions" : [
        {
          "iterator kind" : "FUNC_COUNT_STAR"
        }
      ]
    }
  },
  "grouping expressions" : [
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "number",
      "input iterator" :
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$gb-2"
      }
    }
  ],
  "aggregate functions" : [
    {
      "iterator kind" : "FUNC_SUM",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "cnt",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$gb-2"
        }
      }
    }
  ]
}
}