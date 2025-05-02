compiled-query-plan

{
"query file" : "joins/q/lind04.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "A",
      "row variable" : "$a",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"ida":0},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "A.B", "row variable" : "$$b", "covering primary index" : true },
        { "table" : "A.B.C", "row variable" : "$c", "covering primary index" : true },
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : true }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$a", "$$b", "$c", "$$d"],
    "SELECT expressions" : [
      {
        "field name" : "a",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$a"
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
            "variable" : "$$b"
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
        "field name" : "Column_4",
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
        "field name" : "Column_5",
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
        "field name" : "idc",
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
        "field name" : "Column_7",
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
        "field name" : "Column_8",
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
        "field name" : "Column_9",
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
        "field name" : "idd",
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