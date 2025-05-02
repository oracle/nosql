compiled-query-plan

{
"query file" : "geo/q/near11.q",
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
        "target table" : "points",
        "row variable" : "$$p",
        "index used" : "idx_kind_ptn",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b1e", "start inclusive" : true, "end value" : "sw2b1e", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b1g", "start inclusive" : true, "end value" : "sw2b1g", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b1s", "start inclusive" : true, "end value" : "sw2b1zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b38", "start inclusive" : true, "end value" : "sw2b3gzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b3s", "start inclusive" : true, "end value" : "sw2b3zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b45", "start inclusive" : true, "end value" : "sw2b45", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b47", "start inclusive" : true, "end value" : "sw2b47", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b4e", "start inclusive" : true, "end value" : "sw2b4e", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b4g", "start inclusive" : true, "end value" : "sw2b4zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b55", "start inclusive" : true, "end value" : "sw2b55", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b57", "start inclusive" : true, "end value" : "sw2b57", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b5e", "start inclusive" : true, "end value" : "sw2b5e", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b5g", "start inclusive" : true, "end value" : "sw2b7zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2b98", "start inclusive" : true, "end value" : "sw2b9gzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bd0", "start inclusive" : true, "end value" : "sw2bdgzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2be0", "start inclusive" : true, "end value" : "sw2begzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bh5", "start inclusive" : true, "end value" : "sw2bh5", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bhh", "start inclusive" : true, "end value" : "sw2bhjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bhn", "start inclusive" : true, "end value" : "sw2bhpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bk0", "start inclusive" : true, "end value" : "sw2bk1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bk4", "start inclusive" : true, "end value" : "sw2bk5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bkh", "start inclusive" : true, "end value" : "sw2bkjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bkn", "start inclusive" : true, "end value" : "sw2bkpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bs0", "start inclusive" : true, "end value" : "sw2bs1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw2bs4", "start inclusive" : true, "end value" : "sw2bs5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31c3", "start inclusive" : true, "end value" : "sw31c3", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31c6", "start inclusive" : true, "end value" : "sw31c7zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31c9", "start inclusive" : true, "end value" : "sw31c9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31cc", "start inclusive" : true, "end value" : "sw31cgzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31ck", "start inclusive" : true, "end value" : "sw31cmzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31cq", "start inclusive" : true, "end value" : "sw31czzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31f1", "start inclusive" : true, "end value" : "sw31f1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31f3", "start inclusive" : true, "end value" : "sw31f7zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31f9", "start inclusive" : true, "end value" : "sw31f9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31fc", "start inclusive" : true, "end value" : "sw31fzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31g1", "start inclusive" : true, "end value" : "sw31g1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31g3", "start inclusive" : true, "end value" : "sw31g7zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31g9", "start inclusive" : true, "end value" : "sw31g9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw31gc", "start inclusive" : true, "end value" : "sw31gzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3412", "start inclusive" : true, "end value" : "sw3413zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3416", "start inclusive" : true, "end value" : "sw341gzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw341k", "start inclusive" : true, "end value" : "sw341mzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw341q", "start inclusive" : true, "end value" : "sw341zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3432", "start inclusive" : true, "end value" : "sw3433zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3438", "start inclusive" : true, "end value" : "sw343czzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3440", "start inclusive" : true, "end value" : "sw3463zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3468", "start inclusive" : true, "end value" : "sw346czzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3470", "start inclusive" : true, "end value" : "sw3473zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"hotel"},
            "range conditions" : { "info.point" : { "start value" : "sw3478", "start inclusive" : true, "end value" : "sw347czzzz", "end inclusive" : true } }
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
          "value" : {"coordinates":[[24.0175,35.5156],[23.6793,35.2203]],"type":"multipoint"}
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
          "field name" : "Column_2",
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
              "value" : {"coordinates":[[24.0175,35.5156],[23.6793,35.2203]],"type":"multipoint"}
            }
          }
        }
      ]
    }
  }
}
}