compiled-query-plan

{
"query file" : "geo/q/int03.q",
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
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "idx_ptn",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443tc", "start inclusive" : true, "end value" : "sw3443tc", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443tf", "start inclusive" : true, "end value" : "sw3443tgzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443tu", "start inclusive" : true, "end value" : "sw3443tvzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443ty", "start inclusive" : true, "end value" : "sw3443tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443vb", "start inclusive" : true, "end value" : "sw3443vczz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443vf", "start inclusive" : true, "end value" : "sw3443vgzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443vu", "start inclusive" : true, "end value" : "sw3443vvzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443vy", "start inclusive" : true, "end value" : "sw3443vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443w1", "start inclusive" : true, "end value" : "sw3443w1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443w3", "start inclusive" : true, "end value" : "sw3443w7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443w9", "start inclusive" : true, "end value" : "sw3443w9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443wc", "start inclusive" : true, "end value" : "sw3443wzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443x1", "start inclusive" : true, "end value" : "sw3443x1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443x3", "start inclusive" : true, "end value" : "sw3443x7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443x9", "start inclusive" : true, "end value" : "sw3443x9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443xc", "start inclusive" : true, "end value" : "sw3443zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446jb", "start inclusive" : true, "end value" : "sw3446jczz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446jf", "start inclusive" : true, "end value" : "sw3446jgzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446ju", "start inclusive" : true, "end value" : "sw3446jvzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446jy", "start inclusive" : true, "end value" : "sw3446jzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446mb", "start inclusive" : true, "end value" : "sw3446mb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446n0", "start inclusive" : true, "end value" : "sw3446q0zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446q2", "start inclusive" : true, "end value" : "sw3446q2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446q8", "start inclusive" : true, "end value" : "sw3446q8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446qb", "start inclusive" : true, "end value" : "sw3446qb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446r0", "start inclusive" : true, "end value" : "sw3446r0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446r2", "start inclusive" : true, "end value" : "sw3446r2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446r8", "start inclusive" : true, "end value" : "sw3446r8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3446rb", "start inclusive" : true, "end value" : "sw3446rb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344981", "start inclusive" : true, "end value" : "sw344981", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344983", "start inclusive" : true, "end value" : "sw344987zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344989", "start inclusive" : true, "end value" : "sw344989", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34498c", "start inclusive" : true, "end value" : "sw34498zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344991", "start inclusive" : true, "end value" : "sw344991", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344993", "start inclusive" : true, "end value" : "sw344997zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344999", "start inclusive" : true, "end value" : "sw344999", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34499c", "start inclusive" : true, "end value" : "sw3449czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449d1", "start inclusive" : true, "end value" : "sw3449d1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449d3", "start inclusive" : true, "end value" : "sw3449d7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449d9", "start inclusive" : true, "end value" : "sw3449d9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449dc", "start inclusive" : true, "end value" : "sw3449dzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449e1", "start inclusive" : true, "end value" : "sw3449e1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449e3", "start inclusive" : true, "end value" : "sw3449e7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449e9", "start inclusive" : true, "end value" : "sw3449e9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449ec", "start inclusive" : true, "end value" : "sw3449gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449s1", "start inclusive" : true, "end value" : "sw3449s1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449s3", "start inclusive" : true, "end value" : "sw3449s7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449s9", "start inclusive" : true, "end value" : "sw3449s9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449sc", "start inclusive" : true, "end value" : "sw3449szzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449t1", "start inclusive" : true, "end value" : "sw3449t1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449t3", "start inclusive" : true, "end value" : "sw3449t7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449t9", "start inclusive" : true, "end value" : "sw3449t9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449tc", "start inclusive" : true, "end value" : "sw3449vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449w1", "start inclusive" : true, "end value" : "sw3449w1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449w4", "start inclusive" : true, "end value" : "sw3449w5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449wh", "start inclusive" : true, "end value" : "sw3449wjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449wn", "start inclusive" : true, "end value" : "sw3449wpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449y0", "start inclusive" : true, "end value" : "sw3449y1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449y4", "start inclusive" : true, "end value" : "sw3449y5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449yh", "start inclusive" : true, "end value" : "sw3449yjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3449yn", "start inclusive" : true, "end value" : "sw3449ypzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d00", "start inclusive" : true, "end value" : "sw344d20zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d22", "start inclusive" : true, "end value" : "sw344d22", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d28", "start inclusive" : true, "end value" : "sw344d28", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d2b", "start inclusive" : true, "end value" : "sw344d2b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d30", "start inclusive" : true, "end value" : "sw344d30", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d32", "start inclusive" : true, "end value" : "sw344d32", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d38", "start inclusive" : true, "end value" : "sw344d38", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d3b", "start inclusive" : true, "end value" : "sw344d3b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d40", "start inclusive" : true, "end value" : "sw344d60zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d62", "start inclusive" : true, "end value" : "sw344d62", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d68", "start inclusive" : true, "end value" : "sw344d68", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d6b", "start inclusive" : true, "end value" : "sw344d6b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d70", "start inclusive" : true, "end value" : "sw344d70", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d72", "start inclusive" : true, "end value" : "sw344d72", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d78", "start inclusive" : true, "end value" : "sw344d78", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344d7b", "start inclusive" : true, "end value" : "sw344d7b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dh0", "start inclusive" : true, "end value" : "sw344dk0zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dk2", "start inclusive" : true, "end value" : "sw344dk2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dk8", "start inclusive" : true, "end value" : "sw344dk8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dkb", "start inclusive" : true, "end value" : "sw344dkb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dm0", "start inclusive" : true, "end value" : "sw344dm0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dm2", "start inclusive" : true, "end value" : "sw344dm2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dm8", "start inclusive" : true, "end value" : "sw344dm8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dmb", "start inclusive" : true, "end value" : "sw344dmb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dn0", "start inclusive" : true, "end value" : "sw344dn1zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dn4", "start inclusive" : true, "end value" : "sw344dn5zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dnh", "start inclusive" : true, "end value" : "sw344dnjzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dnn", "start inclusive" : true, "end value" : "sw344dnpzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw344dq0", "start inclusive" : true, "end value" : "sw344dq0", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "WHERE" : 
    {
      "iterator kind" : "FN_GEO_INTERSECT",
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
        "value" : {"coordinates":[[[24.013335,35.518654],[24.016038,35.516646],[24.017927,35.516332],[24.018742,35.517676],[24.024471,35.518114],[24.023699,35.520139],[24.016296,35.520191],[24.013335,35.518654]]],"type":"polygon"}
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
      }
    ]
  }
}
}