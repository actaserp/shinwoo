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
      ),
      -- 2) 현재고(창고 합계)
      stock AS (
        SELECT
          h."Material_id" AS mat_id,
          COALESCE(SUM(h."CurrentStock"), 0)::numeric AS current_stock
        FROM mat_in_house h
        GROUP BY h."Material_id"
      ),
      -- 3) 표시/판단 공통 기초
      base AS (
        SELECT
          m.code                                  AS material_code,
          m.name                                  AS material_name,
          COALESCE(u."Name", m.packing_unit_name) AS unit_name,
          COALESCE(d.order_qty,0)                 AS order_qty,
          m.avrqty_num                            AS optimal_stock,
          COALESCE(s.current_stock, 0)            AS current_stock,
          -- 원시 격차(음수 가능): 수주 + 적정재고 - 현재고
          (COALESCE(d.order_qty,0) + m.avrqty_num - COALESCE(s.current_stock,0)) AS raw_gap
        FROM mat m
        LEFT JOIN stock s        ON s.mat_id = m.id
        LEFT JOIN order_demand d ON d.mat_id = m.id
        LEFT JOIN "unit" u       ON u.id     = m."Unit_id"
        WHERE m.spjangcd = :spjangcd
      )
      SELECT
        material_code,
        material_name,
        unit_name,
        order_qty,
        current_stock,
        optimal_stock,
        GREATEST(raw_gap, 0) AS need_more_qty,
        CASE
          WHEN raw_gap > 0 THEN '부족'
          WHEN raw_gap = 0 THEN '적정'
          ELSE '여유'
        END AS state
      FROM base
      WHERE 1 = 1
      """;

    // 품명(키워드) 필터  ← 바깥 스코프 컬럼명 사용
    if (matName != null && !matName.isEmpty()) {
      sql += " AND material_name ILIKE :matName ";
      paramMap.addValue("matName", "%" + matName + "%");
    }

    // 상태 필터  ← raw_gap 기준으로 동일 CASE 재작성
    if (status != null && !status.isBlank() && !"전체".equals(status)) {
      String st = status.trim();
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
        default:  break;
      }
      sql += """
          AND (
            CASE
              WHEN raw_gap > 0 THEN '부족'
              WHEN raw_gap = 0 THEN '적정'
              ELSE '여유'
            END
          ) = :status
          """;
      paramMap.addValue("status", st);
    }

    sql += " ORDER BY material_code";

//    log.info("paramMap:{}", paramMap);
//    log.info("수주량 대비 적정재고(Avrqty) 현황 sql:{}", sql);

    return sqlRunner.getRows(sql, paramMap);
  }


}
