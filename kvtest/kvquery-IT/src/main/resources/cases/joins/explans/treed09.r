compiled-query-plan

{
"query file" : "joins/q/treed09.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
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
        "row variable" : "$$a",
        "index used" : "a_idx_a1_a2_c1",
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
          { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : false },
          { "table" : "A.G.J.K", "row variable" : "$$k", "covering primary index" : true }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 30
          }
        },
        "position in join" : 0
      },
      "FROM variables" : ["$$a_idx", "$$b", "$$c", "$$k"],
      "SELECT expressions" : [
        {
          "field name" : "a_ida",
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
          "field name" : "a1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$a_idx"
            }
          }
        },
        {
          "field name" : "b_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$b"
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
              "variable" : "$$b"
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
              "variable" : "$$c"
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
              "variable" : "$$c"
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
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "c1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "c1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        },
        {
          "field name" : "k_ida",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ida",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$k"
            }
          }
        },
        {
          "field name" : "k_idg",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idg",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$k"
            }
          }
        },
        {
          "field name" : "k_idj",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idj",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$k"
            }
          }
        },
        {
          "field name" : "k_idk",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "idk",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$k"
            }
          }
        }
      ]
    }
  }
}
}