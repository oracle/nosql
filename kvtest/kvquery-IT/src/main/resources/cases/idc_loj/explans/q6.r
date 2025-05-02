compiled-query-plan

{
"query file" : "idc_loj/q/q6.q",
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
        { "table" : "A.G", "row variable" : "$$g", "covering primary index" : true },
        { "table" : "A.G.H", "row variable" : "$$h", "covering primary index" : true },
        { "table" : "A.G.H.I", "row variable" : "$$i", "covering primary index" : true },
        { "table" : "A.G.H.I.J", "row variable" : "$$j", "covering primary index" : true },
        { "table" : "A.G.H.I.J.K", "row variable" : "$$k", "covering primary index" : true },
        { "table" : "A.G.H.I.J.K.L", "row variable" : "$$l", "covering primary index" : true }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "NOT_EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "A11_Dgp2YJJdeO"
        }
      },
      "ON Predicate for table A.G.H.I" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          }
        ]
      },
      "ON Predicate for table A.G.H.I.J" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          }
        ]
      },
      "ON Predicate for table A.G.H.I.J.K" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$k"
              }
            }
          }
        ]
      },
      "ON Predicate for table A.G.H.I.J.K.L" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$a"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idj1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idj1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idi1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$j"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idh2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$i"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "idg1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$h"
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$l"
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ida1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$g"
              }
            }
          }
        ]
      },
      "position in join" : 0
    },
    "FROM variables" : ["$$a", "$$g", "$$h", "$$i", "$$j", "$$k", "$$l"],
    "SELECT expressions" : [
      {
        "field name" : "a_ida1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "ida1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$a"
          }
        }
      },
      {
        "field name" : "j_idj1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idj1",
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