package mes.app.balju.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BalJuOptimalStockService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getList(String matName, String status, Timestamp start, Timestamp end, String spjangcd) {
    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("matName", matName);
    paramMap.addValue("status", status);
    paramMap.addValue("start", start);
    paramMap.addValue("end", end);
    paramMap.addValue("spjangcd", spjangcd);

    String sql = """
        WITH
        -- 0) 자재 마스터 (+ Avrqty 숫자화)
        mat AS (
          SELECT
            m.id,
            m."Code"  AS code,
            m."Name"  AS name,
            m."Unit_id",
            m.spjangcd,
            COALESCE(m."PackingUnitName",'') AS packing_unit_name,
            COALESCE(m."SafetyStock", 0)::numeric AS safety_stock,
            COALESCE(NULLIF(regexp_replace(m."Avrqty", '[^0-9.\\-]', '', 'g'), ''), '0')::numeric AS avrqty_num
          FROM material m
        ),
        -- 1) 수주량 집계 (헤더 날짜 기준: suju_head."JumunDate")
        order_demand AS (
          SELECT
            li."Material_id" AS mat_id,
            SUM(
              CASE
                WHEN COALESCE(li."SujuQty2",0) > 0 THEN li."SujuQty2"
                ELSE COALESCE(li."SujuQty",0)
              END
            )::numeric AS order_qty
          FROM suju_head hd
          JOIN suju li ON li."SujuHead_id" = hd.id
          WHERE hd.spjangcd = :spjangcd
            AND hd."JumunDate" >= COALESCE(CAST(:start AS date), hd."JumunDate")
            AND hd."JumunDate" <= COALESCE(CAST(:end   AS date), hd."JumunDate")
          GROUP BY li."Material_id"
        )
        SELECT
          m.code                                  AS "material_code",
          m.name                                  AS "material_name",
          COALESCE(u."Name", m.packing_unit_name) AS "unit_name",
          -- 수주량(기간 합계)
          COALESCE(d.order_qty,0)                 AS "order_qty",
          -- 적정재고(Avrqty)
          m.avrqty_num                            AS "optimal_stock",
          -- 차이(부족분) = 수주량 - 적정재고
          (COALESCE(d.order_qty,0) - m.avrqty_num)            AS "diff_order",
          GREATEST(COALESCE(d.order_qty,0) - m.avrqty_num, 0) AS "need_more_qty",
          -- 상태(수주량 기준)
          CASE
            WHEN COALESCE(d.order_qty,0) >  m.avrqty_num THEN '부족'
            WHEN COALESCE(d.order_qty,0) =  m.avrqty_num THEN '적정'
            ELSE '여유'
          END                                        AS "state"
        FROM mat m
        LEFT JOIN order_demand d ON d.mat_id = m.id
        LEFT JOIN "unit" u       ON u.id     = m."Unit_id"
        WHERE m.spjangcd = :spjangcd
        """;

    // 품명(키워드) 필터
    if (matName != null && !matName.isEmpty()) {
      sql += " AND m.name ILIKE :matName ";
      paramMap.addValue("matName", "%" + matName + "%");
    }

    // 상태 필터: '부족' / '적정' / '여유' (영문 키워드도 매핑)
    if (status != null && !status.isBlank() && !"전체".equals(status)) {
      String st = status.trim();
      // 영문 → 한글 매핑
      switch (st.toLowerCase()) {
        case "shortage":
        case "lack":
        case "insufficient":
        case "tribe":   st = "부족"; break;
        case "proper":
        case "ok":
        case "equal":   st = "적정"; break;
        case "excess":
        case "surplus": st = "여유"; break;
        default: /* 한글 그대로(부족/적정/여유) 들어오면 사용 */ break;
      }
      sql += """
            AND (
              CASE
                WHEN COALESCE(d.order_qty,0) >  m.avrqty_num THEN '부족'
                WHEN COALESCE(d.order_qty,0) =  m.avrqty_num THEN '적정'
                ELSE '여유'
              END
            ) = :status
            """;
      paramMap.addValue("status", st);
    }

    sql += " ORDER BY m.code ";

//    log.info("paramMap:{}", paramMap);
//    log.info("수주량 대비 적정재고(Avrqty) 현황 sql:{}", sql);

    return sqlRunner.getRows(sql, paramMap);
  }


}
