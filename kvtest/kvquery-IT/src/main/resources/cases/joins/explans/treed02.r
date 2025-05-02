compiled-query-plan

{
"query file" : "joins/q/treed02.q",
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
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : true },
        { "table" : "A.G.J", "row variable" : "$$j", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$d", "$$j"],
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
      },
      {
        "field name" : "j_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$j"
          }
        }
      },
      {
        "field name" : "j_idg",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idg",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$j"
          }
        }
      },
      {
        "field name" : "j_idj",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idj",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$j"
          }
        }
      }
    ]
  }
}
}