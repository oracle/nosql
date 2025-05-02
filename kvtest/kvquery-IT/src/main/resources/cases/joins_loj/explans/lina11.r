compiled-query-plan

{
"query file" : "joins_loj/q/lina11.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A.B.C.D",
      "row variable" : "$$d",
      "index used" : "d_idx_d2_idb_c3",
      "covering index" : false,
      "index row variable" : "$$d_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "d2" : { "start value" : 15, "start inclusive" : false } }
        }
      ],
      "ancestor tables" : [
        { "table" : "A", "row variable" : "$$a", "covering primary index" : false },
        { "table" : "A.B", "row variable" : "$$b", "covering primary index" : false },
        { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : true }      ],
      "index filtering predicate" :
      {
        "iterator kind" : "LESS_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 100
        }
      },
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$b", "$$c", "$$d"],
    "SELECT expressions" : [
      {
        "field name" : "d_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
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
            "variable" : "$$d"
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
            "variable" : "$$d"
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
            "variable" : "$$d"
          }
        }
      },
      {
        "field name" : "d1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "d1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
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
            "variable" : "$$a"
          }
        }
      },
      {
        "field name" : "a_c1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
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
        "field name" : "b_c1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "c1",
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
      }
    ]
  }
}
}