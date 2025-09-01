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
        -- 0) 자재 마스터(적정재고/현재고/단위)
        mat AS (
          SELECT
            m.id,
            m."Code"  AS material_code,
            m."Name"  AS material_name,
            COALESCE(u."Name",'') AS unit_name,
            m.spjangcd,
            COALESCE(m."CurrentStock",0)::numeric AS current_stock,
            COALESCE(
              NULLIF(regexp_replace(m."Avrqty", '[^0-9.-]', '', 'g'), ''),
              '0'
            )::numeric AS optimal_stock
          FROM material m
          LEFT JOIN unit u ON u.id = m."Unit_id"
          WHERE m.spjangcd = :spjangcd
            AND m."Useyn" = '0'
        ),
        -- 1) 수주 집계(납품예정일 기준: head.DeliveryDate 우선, 없으면 line.DueDate)
        orders AS (
          SELECT
            s."Material_id" AS material_id,
            s."Standard"    AS standard,
            SUM(
              CASE WHEN COALESCE(s."SujuQty2",0) > 0 THEN s."SujuQty2"
                   ELSE COALESCE(s."SujuQty",0) END
            )::numeric AS order_qty
          FROM suju_head h
          JOIN suju s ON s."SujuHead_id" = h.id
          WHERE h.spjangcd = :spjangcd
            AND COALESCE(h."DeliveryDate", s."DueDate")
                BETWEEN CAST(:start AS date) AND CAST(:end AS date)
          GROUP BY s."Material_id", s."Standard"
        ),
        -- 2) 표시 대상(수주 존재 자재만)  ※ incoming 제외
        base AS (
          SELECT
            m.id AS material_id,
            m.material_code,
            m.material_name,
            o.standard,
            m.unit_name,
            COALESCE(o.order_qty,0) AS order_qty,
            0::numeric              AS incoming_qty,   -- ← 계산 제외(표시만 0)
            m.current_stock,
            m.optimal_stock
          FROM orders o
          JOIN mat m ON m.id = o.material_id
        )
        SELECT *
        FROM (
          SELECT
            material_code,
            material_name,
            standard,
            unit_name,
            order_qty,
            current_stock,
            optimal_stock,
            /* 필요수량 = (수주 + 적정재고) - 현재고 의 양수부 */
            GREATEST(
              (COALESCE(order_qty,0) + COALESCE(optimal_stock,0)) - COALESCE(current_stock,0),
              0
            )::numeric AS need_more_qty,
            CASE
              WHEN COALESCE(current_stock,0) - (COALESCE(order_qty,0) + COALESCE(optimal_stock,0)) < 0 THEN '부족'
              WHEN COALESCE(current_stock,0) - (COALESCE(order_qty,0) + COALESCE(optimal_stock,0)) = 0 THEN '적정'
              ELSE '여유'
            END AS state
          FROM base
        ) t
        WHERE 1=1 
      """;

    // 품명(키워드) 필터: 이름/코드 모두 검색
    if (matName != null && !matName.isEmpty()) {
      sql += " AND (t.material_name ILIKE :matName OR t.material_code ILIKE :matName) ";
      paramMap.addValue("matName", "%" + matName + "%");
    }

    // 상태 필터
    if (status != null && !status.isBlank() && !"전체".equals(status.trim())) {
      String st = status.trim();
      switch (st.toLowerCase()) {
        case "shortage":
        case "lack":
        case "insufficient": st = "부족"; break;
        case "proper":
        case "ok":
        case "equal":       st = "적정"; break;
        case "excess":
        case "surplus":     st = "여유"; break;
        default: break;
      }
      sql += " AND t.state = :status ";
      paramMap.addValue("status", st);
    }

    sql += " ORDER BY t.material_code, COALESCE(t.standard,'')";

//    log.info("paramMap:{}", paramMap);
//    log.info("적정재고 현황(납품예정일 기준) sql:{}", sql);

    return sqlRunner.getRows(sql, paramMap);
  }

}
