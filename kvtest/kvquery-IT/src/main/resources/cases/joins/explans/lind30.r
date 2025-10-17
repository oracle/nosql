compiled-query-plan
{
"query file" : "joins/q/lind30.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1, 2 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "A",
        "row variable" : "$$a",
        "index used" : "a_idx_a2",
        "covering index" : true,
        "index row variable" : "$$a_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "descendant tables" : [
          { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true },
          { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : true }
        ],
        "position in join" : 0
      },
      "FROM variables" : ["$$a_idx", "$$b", "$$c"],
      "GROUP BY" : "Grouping by the first 3 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "a2",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a2",
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
          "field name" : "idb",
          "field expression" : 
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
        {
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FN_COUNT",
            "input iterator" :
            {
              "iterator kind" : "CASE",
              "clauses" : [
                {
                  "when iterator" :
                  {
                    "iterator kind" : "OP_IS_NULL",
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
                  "then iterator" :
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
                {
                  "when iterator" :
                  {
                    "iterator kind" : "OP_IS_NULL",
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "idc",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$$c"
                      }
                    }
                  },
                  "then iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "idc",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$c"
                    }
                  }
                },
                {
                  "else iterator" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 1
                  }
                }
              ]
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 3 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "a2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "a2",
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
      "field name" : "idb",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "idb",
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
