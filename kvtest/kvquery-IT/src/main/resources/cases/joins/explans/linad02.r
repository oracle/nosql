compiled-query-plan

{
"query file" : "joins/q/linad02.q",
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
      "row variable" : "$$b",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "ancestor tables" : [
        { "table" : "A", "row variable" : "$$a", "covering primary index" : false }      ],
      "descendant tables" : [
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$b", "$$d"],
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
        "field name" : "a2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
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