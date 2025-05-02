compiled-query-plan

{
"query file" : "joins/q/linad05.q",
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
        { "table" : "A.B.C.D", "row variable" : "$d", "covering primary index" : false }
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
    "FROM variables" : ["$a", "$b", "$d"],
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
        "field name" : "size_d_idx_d2_idb_c3",
        "field expression" : 
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "LESS_OR_EQUAL",
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