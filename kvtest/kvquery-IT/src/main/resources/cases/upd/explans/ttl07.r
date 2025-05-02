compiled-query-plan

{
"query file" : "upd/q/ttl07.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "UPDATE_ROW",
      "indexes to update" : [  ],
      "update clauses" : [

      ],
      "update TTL" : true,
      "TimeUnit" : "DAYS",
      "TTL iterator" :
      {
        "iterator kind" : "ADD_SUBTRACT",
        "operations and operands" : [
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "FUNC_REMAINING_DAYS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          }
        ]
      },
      "isCompletePrimaryKey" : true,
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$f",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":6},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$f",
        "SELECT expressions" : [
          {
            "field name" : "f",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$f"
            }
          }
        ]
      }
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "CASE",
          "clauses" : [
            {
              "when iterator" :
              {
                "iterator kind" : "EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FUNC_REMAINING_DAYS",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 7
                }
              },
              "then iterator" :
              {
                "iterator kind" : "CONST",
                "value" : true
              }
            },
            {
              "else iterator" :
              {
                "iterator kind" : "FUNC_REMAINING_DAYS",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}
