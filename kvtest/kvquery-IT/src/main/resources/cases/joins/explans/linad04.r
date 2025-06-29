compiled-query-plan

{
"query file" : "joins/q/linad04.q",
"plan" : 
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
      "target table" : "A.B",
      "row variable" : "$b",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "ancestor tables" : [
        { "table" : "A", "row variable" : "$a", "covering primary index" : false }      ],
      "descendant tables" : [
        { "table" : "A.B.C", "row variable" : "$c", "covering primary index" : false }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "NOT_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$b"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 40
        }
      },
      "position in join" : 0
    },
    "FROM variables" : ["$a", "$b", "$c"],
    "SELECT expressions" : [
      {
        "field name" : "b_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$b"
          }
        }
      },
      {
        "field name" : "b_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$b"
          }
        }
      },
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
        "field name" : "c_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$c"
          }
        }
      },
      {
        "field name" : "c_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$c"
          }
        }
      },
      {
        "field name" : "c_idc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$c"
          }
        }
      },
      {
        "field name" : "c_size",
        "field expression" : 
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "LESS_THAN",
              "left operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$a"
                }
              },
              "right operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$b"
                }
              }
            },
            {
              "iterator kind" : "LESS_THAN",
              "left operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$b"
                }
              },
              "right operand" :
              {
                "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$c"
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "size_a_idx_a2",
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
                "value" : 30
              }
            }
          ]
        }
      }
    ]
  }
}
}