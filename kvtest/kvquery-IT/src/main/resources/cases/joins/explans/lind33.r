compiled-query-plan
{
"query file" : "joins/q/lind33.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "order by fields at positions" : [ 0 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "A",
          "row variable" : "$$a",
          "index used" : "primary index",
          "covering index" : true,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "descendant tables" : [
            { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true }
          ],
          "position in join" : 0
        },
        "FROM variables" : ["$$a", "$$b"],
        "WHERE" : 
        {
          "iterator kind" : "OP_IS_NOT_NULL",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
            }
          }
        },
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "ida",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          },
          {
            "field name" : "count",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "idb",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$b"
                }
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$from-1",
    "GROUP BY" : "Grouping by the first expression in the SELECT list",
    "SELECT expressions" : [
      {
        "field name" : "ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "count",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "count",
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
