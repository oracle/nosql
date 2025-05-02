compiled-query-plan

{
"query file" : "joins/q/linad06.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0 ],
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
        "target table" : "A",
        "row variable" : "$a",
        "index used" : "a_idx_c1",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "c1" : { "start value" : 10, "start inclusive" : false } }
          }
        ],
        "descendant tables" : [
          { "table" : "A.B.C.D", "row variable" : "$d", "covering primary index" : false }
        ],
        "position in join" : 0
      },
      "FROM variables" : ["$a", "$d"],
      "SELECT expressions" : [
        {
          "field name" : "a_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$a"
            }
          }
        },
        {
          "field name" : "a1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$a"
            }
          }
        },
        {
          "field name" : "d_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$d"
            }
          }
        },
        {
          "field name" : "d_idb",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idb",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$d"
            }
          }
        },
        {
          "field name" : "d_idc",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idc",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$d"
            }
          }
        },
        {
          "field name" : "d_idd",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idd",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$d"
            }
          }
        },
        {
          "field name" : "size_a_idx_c1",
          "field expression" : 
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 20
                },
                "right operand" :
                {
                  "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$a"
                  }
                }
              },
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$a"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 28
                }
              }
            ]
          }
        },
        {
          "field name" : "size_d_idx_d2_idb_c3",
          "field expression" : 
          {
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 50
                },
                "right operand" :
                {
                  "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$d"
                  }
                }
              },
              {
                "iterator kind" : "LESS_THAN",
                "left operand" :
                {
                  "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$d"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 65
                }
              }
            ]
          }
        }
      ]
    }
  }
}
}