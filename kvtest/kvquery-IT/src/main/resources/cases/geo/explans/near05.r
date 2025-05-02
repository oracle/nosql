compiled-query-plan

{
"query file" : "geo/q/near05.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SORT",
    "order by fields at positions" : [ 2 ],
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
          "target table" : "points",
          "row variable" : "$$p",
          "index used" : "idx_ptn",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31c1", "start inclusive" : true, "end value" : "sw31c1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31c3", "start inclusive" : true, "end value" : "sw31c7zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31c9", "start inclusive" : true, "end value" : "sw31c9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31cc", "start inclusive" : true, "end value" : "sw31czzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31f1", "start inclusive" : true, "end value" : "sw31f1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31f3", "start inclusive" : true, "end value" : "sw31f7zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31f9", "start inclusive" : true, "end value" : "sw31f9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31fc", "start inclusive" : true, "end value" : "sw31fzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31g1", "start inclusive" : true, "end value" : "sw31g1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31g3", "start inclusive" : true, "end value" : "sw31g7zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31g9", "start inclusive" : true, "end value" : "sw31g9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw31gc", "start inclusive" : true, "end value" : "sw31gzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3410", "start inclusive" : true, "end value" : "sw341zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3430", "start inclusive" : true, "end value" : "sw3434zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3436", "start inclusive" : true, "end value" : "sw3436", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3438", "start inclusive" : true, "end value" : "sw343dzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw343f", "start inclusive" : true, "end value" : "sw343f", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3440", "start inclusive" : true, "end value" : "sw3464zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3466", "start inclusive" : true, "end value" : "sw3466", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3468", "start inclusive" : true, "end value" : "sw346dzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw346f", "start inclusive" : true, "end value" : "sw346f", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3470", "start inclusive" : true, "end value" : "sw3474zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3476", "start inclusive" : true, "end value" : "sw3476", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3478", "start inclusive" : true, "end value" : "sw347dzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw347f", "start inclusive" : true, "end value" : "sw347f", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bf9", "start inclusive" : true, "end value" : "sw3bf9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bfc", "start inclusive" : true, "end value" : "sw3bfgzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bfs", "start inclusive" : true, "end value" : "sw3bfzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bg1", "start inclusive" : true, "end value" : "sw3bg1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bg3", "start inclusive" : true, "end value" : "sw3bg7zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bg9", "start inclusive" : true, "end value" : "sw3bg9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bgc", "start inclusive" : true, "end value" : "sw3bgzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bu1", "start inclusive" : true, "end value" : "sw3bu1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bu3", "start inclusive" : true, "end value" : "sw3bu7zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bu9", "start inclusive" : true, "end value" : "sw3bu9", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3buc", "start inclusive" : true, "end value" : "sw3buzzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bv1", "start inclusive" : true, "end value" : "sw3bv1", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bv4", "start inclusive" : true, "end value" : "sw3bv5zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bvh", "start inclusive" : true, "end value" : "sw3bvjzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3bvn", "start inclusive" : true, "end value" : "sw3bvpzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3c48", "start inclusive" : true, "end value" : "sw3c4gzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3c4s", "start inclusive" : true, "end value" : "sw3c5zzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3c68", "start inclusive" : true, "end value" : "sw3c6czzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3c70", "start inclusive" : true, "end value" : "sw3c73zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3c78", "start inclusive" : true, "end value" : "sw3c7czzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3ch0", "start inclusive" : true, "end value" : "sw3cj1zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3cj4", "start inclusive" : true, "end value" : "sw3cj5zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3cjh", "start inclusive" : true, "end value" : "sw3cjjzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3cjn", "start inclusive" : true, "end value" : "sw3cjpzzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3ck0", "start inclusive" : true, "end value" : "sw3ck3zzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3ck8", "start inclusive" : true, "end value" : "sw3ckczzzz", "end inclusive" : true } }
            },
            {
              "equality conditions" : {},
              "range conditions" : { "info.point" : { "start value" : "sw3cm0", "start inclusive" : true, "end value" : "sw3cm1zzzz", "end inclusive" : true } }
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$p",
        "WHERE" : 
        {
          "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
          "search target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "point",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          "search geometry iterator" :
          {
            "iterator kind" : "CONST",
            "value" : {"geometries":[{"coordinates":[25.134522,35.338772],"type":"point"},{"coordinates":[[[24.013335,35.518654],[24.016038,35.516646],[24.017927,35.516332],[24.018742,35.517676],[24.024471,35.518114],[24.023699,35.520139],[24.016296,35.520191],[24.013335,35.518654]]],"type":"polygon"}],"type":"GeometryCollection"}
          },
          "distance iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 5000.0
          }
        },
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$p"
              }
            }
          },
          {
            "field name" : "point",
            "field expression" : 
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : true,
              "input iterators" : [
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "point",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "info",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$p"
                    }
                  }
                }
              ]
            }
          },
          {
            "field name" : "sort_gen",
            "field expression" : 
            {
              "iterator kind" : "GEO_DISTANCE",
              "first geometry iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "point",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$p"
                  }
                }
              },
              "second geometry iterator" :
              {
                "iterator kind" : "CONST",
                "value" : {"geometries":[{"coordinates":[25.134522,35.338772],"type":"point"},{"coordinates":[[[24.013335,35.518654],[24.016038,35.516646],[24.017927,35.516332],[24.018742,35.517676],[24.024471,35.518114],[24.023699,35.520139],[24.016296,35.520191],[24.013335,35.518654]]],"type":"polygon"}],"type":"GeometryCollection"}
              }
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "point",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "point",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}