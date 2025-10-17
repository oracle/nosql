compiled-query-plan
{
"query file" : "joins/q/lind35.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A",
        "row variable" : "$$a",
        "index used" : "a_idx_c1",
        "covering index" : true,
        "index row variable" : "$$a_idx",
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
      "FROM variables" : ["$$a_idx", "$$b"],
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "c1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        },
        {
          "field name" : "ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        },
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "c1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "c1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
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
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
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
