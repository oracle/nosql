compiled-query-plan

{
"query file" : "joins/q/treed06.q",
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
        { "table" : "A.B.C.D", "row variable" : "$$d", "covering primary index" : false },
        { "table" : "A.B.E", "row variable" : "$$e", "covering primary index" : true },
        { "table" : "A.F", "row variable" : "$$f", "covering primary index" : false },
        { "table" : "A.G.H", "row variable" : "$$h", "covering primary index" : true },
        { "table" : "A.G.J", "row variable" : "$$j", "covering primary index" : true }
      ],
      "ON Predicate for table A.B.C.D" : 
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "d1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$d"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 10
        }
      },
      "ON Predicate for table A.F" : 
      {
        "iterator kind" : "LESS_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "f2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 90
        }
      },
      "ON Predicate for table A.G.H" : 
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idh",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$h"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 25
        }
      },
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$d", "$$e", "$$f", "$$h", "$$j"],
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
        "field name" : "e_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$e"
          }
        }
      },
      {
        "field name" : "e_idb",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idb",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$e"
          }
        }
      },
      {
        "field name" : "e_ide",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ide",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$e"
          }
        }
      },
      {
        "field name" : "f_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      },
      {
        "field name" : "f_idf",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idf",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
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
      },
      {
        "field name" : "h_ida",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$h"
          }
        }
      },
      {
        "field name" : "h_idg",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idg",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$h"
          }
        }
      },
      {
        "field name" : "h_idh",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idh",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$h"
          }
        }
      }
    ]
  }
}
}