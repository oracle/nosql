compiled-query-plan

{
"query file" : "idc_geojson/q/q21.q",
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
          "range conditions" : { "info.point" : { "start value" : "w0p05", "start inclusive" : true, "end value" : "w0p05", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p07", "start inclusive" : true, "end value" : "w0p07", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p0e", "start inclusive" : true, "end value" : "w0p0e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p0g", "start inclusive" : true, "end value" : "w0p0zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p15", "start inclusive" : true, "end value" : "w0p15", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p17", "start inclusive" : true, "end value" : "w0p17", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p1e", "start inclusive" : true, "end value" : "w0p1e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p1g", "start inclusive" : true, "end value" : "w0p3zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p45", "start inclusive" : true, "end value" : "w0p45", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p47", "start inclusive" : true, "end value" : "w0p47", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p4e", "start inclusive" : true, "end value" : "w0p4e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p4g", "start inclusive" : true, "end value" : "w0p4zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p55", "start inclusive" : true, "end value" : "w0p55", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p57", "start inclusive" : true, "end value" : "w0p57", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p5e", "start inclusive" : true, "end value" : "w0p5e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0p5g", "start inclusive" : true, "end value" : "w0pb4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pb6", "start inclusive" : true, "end value" : "w0pb6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pb8", "start inclusive" : true, "end value" : "w0pbdzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pbf", "start inclusive" : true, "end value" : "w0pbf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pc0", "start inclusive" : true, "end value" : "w0pc4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pc6", "start inclusive" : true, "end value" : "w0pc6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pc8", "start inclusive" : true, "end value" : "w0pcdzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pcf", "start inclusive" : true, "end value" : "w0pcf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pd0", "start inclusive" : true, "end value" : "w0pf4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pf6", "start inclusive" : true, "end value" : "w0pf6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pf8", "start inclusive" : true, "end value" : "w0pfdzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pff", "start inclusive" : true, "end value" : "w0pff", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pg0", "start inclusive" : true, "end value" : "w0pg4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pg6", "start inclusive" : true, "end value" : "w0pg6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pg8", "start inclusive" : true, "end value" : "w0pgdzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pgf", "start inclusive" : true, "end value" : "w0pgf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0ph5", "start inclusive" : true, "end value" : "w0ph5", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0ph7", "start inclusive" : true, "end value" : "w0ph7", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0phe", "start inclusive" : true, "end value" : "w0phe", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0phg", "start inclusive" : true, "end value" : "w0phzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pj5", "start inclusive" : true, "end value" : "w0pj5", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pj7", "start inclusive" : true, "end value" : "w0pj7", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pje", "start inclusive" : true, "end value" : "w0pje", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pjh", "start inclusive" : true, "end value" : "w0pjtzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pjw", "start inclusive" : true, "end value" : "w0pjxzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pk0", "start inclusive" : true, "end value" : "w0pm9zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pmd", "start inclusive" : true, "end value" : "w0pmezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pmh", "start inclusive" : true, "end value" : "w0pmtzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pmw", "start inclusive" : true, "end value" : "w0pmxzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0ps0", "start inclusive" : true, "end value" : "w0pt9zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0ptd", "start inclusive" : true, "end value" : "w0ptezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pth", "start inclusive" : true, "end value" : "w0pttzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0ptw", "start inclusive" : true, "end value" : "w0ptxzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pu0", "start inclusive" : true, "end value" : "w0pu4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pu6", "start inclusive" : true, "end value" : "w0pu6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pu8", "start inclusive" : true, "end value" : "w0pudzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0puf", "start inclusive" : true, "end value" : "w0puf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pv0", "start inclusive" : true, "end value" : "w0pv4zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pv6", "start inclusive" : true, "end value" : "w0pv6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pv8", "start inclusive" : true, "end value" : "w0pv9zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w0pvd", "start inclusive" : true, "end value" : "w0pvd", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22e3", "start inclusive" : true, "end value" : "w22e3", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22e6", "start inclusive" : true, "end value" : "w22e7zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22e9", "start inclusive" : true, "end value" : "w22e9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22ec", "start inclusive" : true, "end value" : "w22egzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22ek", "start inclusive" : true, "end value" : "w22emzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22eq", "start inclusive" : true, "end value" : "w22ezzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22g2", "start inclusive" : true, "end value" : "w22g3zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22g6", "start inclusive" : true, "end value" : "w22ggzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22gk", "start inclusive" : true, "end value" : "w22gmzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22gq", "start inclusive" : true, "end value" : "w22gzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22s1", "start inclusive" : true, "end value" : "w22s1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22s3", "start inclusive" : true, "end value" : "w22s7zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22s9", "start inclusive" : true, "end value" : "w22s9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22sc", "start inclusive" : true, "end value" : "w22szzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22t1", "start inclusive" : true, "end value" : "w22t1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22t3", "start inclusive" : true, "end value" : "w22t7zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22t9", "start inclusive" : true, "end value" : "w22t9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22tc", "start inclusive" : true, "end value" : "w22vzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22w1", "start inclusive" : true, "end value" : "w22w1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22w3", "start inclusive" : true, "end value" : "w22w7zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22w9", "start inclusive" : true, "end value" : "w22w9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22wc", "start inclusive" : true, "end value" : "w22wzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22x1", "start inclusive" : true, "end value" : "w22x1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22x3", "start inclusive" : true, "end value" : "w22x7zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22x9", "start inclusive" : true, "end value" : "w22x9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w22xc", "start inclusive" : true, "end value" : "w22zzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2352", "start inclusive" : true, "end value" : "w2353zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2356", "start inclusive" : true, "end value" : "w235gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w235k", "start inclusive" : true, "end value" : "w235mzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w235q", "start inclusive" : true, "end value" : "w235zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w23h0", "start inclusive" : true, "end value" : "w23jzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w23n0", "start inclusive" : true, "end value" : "w23pzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2881", "start inclusive" : true, "end value" : "w2881", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2883", "start inclusive" : true, "end value" : "w2887zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2889", "start inclusive" : true, "end value" : "w2889", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w288c", "start inclusive" : true, "end value" : "w288zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2891", "start inclusive" : true, "end value" : "w2891", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2894", "start inclusive" : true, "end value" : "w2895zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w289h", "start inclusive" : true, "end value" : "w289jzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w289n", "start inclusive" : true, "end value" : "w289pzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w28b0", "start inclusive" : true, "end value" : "w28c1zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w28c4", "start inclusive" : true, "end value" : "w28c5zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w28ch", "start inclusive" : true, "end value" : "w28cjzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w28cn", "start inclusive" : true, "end value" : "w28cpzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2900", "start inclusive" : true, "end value" : "w2911zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w2914", "start inclusive" : true, "end value" : "w2915zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w291h", "start inclusive" : true, "end value" : "w291jzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "w291n", "start inclusive" : true, "end value" : "w291pzzzzz", "end inclusive" : true } }
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
        "value" : {"coordinates":[[[[102.0,2.0],[103.0,2.0],[103.0,3.0],[102.0,3.0],[102.0,2.0]]],[[[100.0,0.0],[101.0,0.0],[101.0,1.0],[100.0,1.0],[100.0,0.0]]]],"type":"MultiPolygon"}
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