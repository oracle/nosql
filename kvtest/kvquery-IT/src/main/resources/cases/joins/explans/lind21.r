compiled-query-plan

{
"query file" : "joins/q/lind21.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A",
      "row variable" : "$$a",
      "index used" : "a_idx_c1_a2",
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
        { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : true },
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a_idx", "$$b", "$$c", "$$d"],
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
      }
    ]
  }
}
}