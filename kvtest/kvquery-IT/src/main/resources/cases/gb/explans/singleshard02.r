compiled-query-plan

{
"query file" : "gb/q/singleshard02.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "SINGLE_PARTITION",
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$$f",
          "index used" : "idx_acc_year_prodcat",
          "covering index" : true,
          "index row variable" : "$$f_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "index filtering predicate" :
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id1",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 0
                }
              },
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "xact.year",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$f_idx"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 2000
                }
              }
            ]
          },
          "position in join" : 0
        },
        "FROM variable" : "$$f_idx",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "acctno",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "xact.acctno",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f_idx"
              }
            }
          },
          {
            "field name" : "Column_2",
            "field expression" : 
            {
              "iterator kind" : "FUNC_COUNT_STAR"
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-1",
    "GROUP BY" : "Grouping by the first expression in the SELECT list",
    "SELECT expressions" : [
      {
        "field name" : "acctno",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "acctno",
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
          "iterator kind" : "FUNC_SUM",
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
    ]
  }
}
}