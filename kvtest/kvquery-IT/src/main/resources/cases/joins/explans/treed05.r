compiled-query-plan

{
"query file" : "joins/q/treed05.q",
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
          "range conditions" : { "ida" : { "start value" : 0, "start inclusive" : false, "end value" : 40, "end inclusive" : false } }
        }
      ],
      "descendant tables" : [
        { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true },
        { "table" : "A.B.C", "row variable" : "$$c", "covering primary index" : true },
        { "table" : "A.G.J.K", "row variable" : "$$k", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$b", "$$c", "$$k"],
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