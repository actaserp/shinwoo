package mes.app.support.service;

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
public class MaterialOptimalStockStatusServicr {
  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getList(String matName, String status, Integer store_id,
                                           Timestamp start, Timestamp end, String spjangcd) {

    MapSqlParameterSource paramMap = new MapSqlParameterSource();
    paramMap.addValue("matName", matName);
    paramMap.addValue("status", status);
    paramMap.addValue("store_id", store_id);
    paramMap.addValue("start", start);
    paramMap.addValue("end", end);
    paramMap.addValue("spjangcd", spjangcd);

    String sql = """
        WITH
        -- 0) 자재 마스터 (+ Avrqty 숫자 변환)
        mat AS (
          SELECT
            m.id,
            m."Code"  AS code,
            m."Name"  AS name,
            m."Unit_id",
            m.spjangcd,
            COALESCE(m."PackingUnitName",'') AS packing_unit_name,
            COALESCE(m."SafetyStock", 0)::numeric AS safety_stock,
            GREATEST(COALESCE(m."LeadTime", 0), 0) AS leadtime_days,
            LOWER(TRIM(COALESCE(m."PurchaseOrderStandard", ''))) AS order_type, -- 표시용
            COALESCE(
              NULLIF(regexp_replace(m."Avrqty", '[^0-9.\\-]', '', 'g'), ''),
              '0'
            )::numeric AS avrqty_num
          FROM material m
        ),
        -- 1) 현재고(창고 합계)
        stock AS (
          SELECT
            h."Material_id" AS mat_id,
            COALESCE(SUM(h."CurrentStock"), 0)::numeric AS current_stock
          FROM mat_in_house h
          GROUP BY h."Material_id"
        ),
        -- 2) 최근 입고일(:start ~ :end 사이의 최종 입고)
        last_receipt_ranked AS (
          SELECT
            io."Material_id"   AS mat_id,
            io."StoreHouse_id" AS store_house_id,
            s."Name"           AS house_name,
            io."InoutDate"     AS last_in_date,
            ROW_NUMBER() OVER (PARTITION BY io."Material_id" ORDER BY io."InoutDate" DESC) AS rn
          FROM mat_inout io
          LEFT JOIN store_house s ON s.id = io."StoreHouse_id"
          WHERE lower(io."InOut") = 'in'
            AND io."InoutDate" >= COALESCE(CAST(:start AS date), io."InoutDate")
            AND io."InoutDate" <= COALESCE(CAST(:end   AS date), io."InoutDate")
        ),
        last_receipt AS (
          SELECT mat_id, store_house_id, house_name, last_in_date
          FROM last_receipt_ranked
          WHERE rn = 1
        ),
        -- 3) BOM 자식 여부 (표시용)
        bom_child AS (
          SELECT DISTINCT bc."Material_id" AS mat_id
          FROM bom_comp bc
        ),
        -- 4) 발주구분 자동결정(표시용)
        order_type_resolved AS (
          SELECT
            m.id,
            CASE
              WHEN m.order_type IN ('mrp','rop') THEN m.order_type
              WHEN bc.mat_id IS NOT NULL         THEN 'mrp'
              ELSE 'rop'
            END AS order_type_resolved
          FROM mat m
          LEFT JOIN bom_child bc ON bc.mat_id = m.id
        )

        SELECT
          m.code                        AS "material_code",
          m.name                        AS "material_name",
          COALESCE(u."Name", m.packing_unit_name) AS "unit_name",
          COALESCE(s.current_stock, 0)  AS "current_stock",

          -- 적정재고: Avrqty 사용
          m.avrqty_num                  AS "optimal_stock",

          -- 차이재고
          (COALESCE(s.current_stock,0) - m.avrqty_num) AS "diff_stock",

          -- 상태
          CASE
            WHEN COALESCE(s.current_stock,0) > m.avrqty_num THEN '과잉'
            WHEN COALESCE(s.current_stock,0) = m.avrqty_num THEN '적정'
            ELSE '부족'
          END AS "state",

          r.last_in_date                AS "last_in_date",
          r.house_name,
          otr.order_type_resolved       AS "order_type_used" -- 표시용

        FROM mat m
        LEFT JOIN stock s                 ON s.mat_id = m.id
        LEFT JOIN last_receipt r          ON r.mat_id = m.id
        LEFT JOIN order_type_resolved otr ON otr.id   = m.id
        LEFT JOIN "unit" u                ON u.id     = m."Unit_id"

        WHERE m.spjangcd = :spjangcd
          -- 입고 이력이 없는 자재도 포함하려면 NULL 허용
          AND (r."last_in_date" BETWEEN :start AND :end OR r."last_in_date" IS NOT NULL)
        """;

    // 품명 필터 (대소문자 구분 없이 검색 원하면 ILIKE 권장: Postgres)
    if (matName != null && !matName.isEmpty()) {
      sql += " AND m.name ILIKE :matName ";
      paramMap.addValue("matName", '%' + matName + '%');
    }

    // 창고 필터: last_receipt를 통해 필터링(입고 이력 없는 행은 제외됨에 유의)
    if (store_id != null) {
      sql += " AND r.store_house_id = :store_id ";
      paramMap.addValue("store_id", store_id);
    }

    // 상태 필터 (Avrqty 기준)
    if (status != null && !status.isBlank() && !"전체".equals(status)) {
      status = status.trim().toLowerCase();
      switch (status) {
        case "excess": status = "과잉"; break;
        case "proper": status = "적정"; break;
        case "tribe":  status = "부족"; break;
        // 한글 그대로 들어오면 그대로 사용
      }
      sql += """
          AND (
            CASE
              WHEN COALESCE(s.current_stock,0) > m.avrqty_num THEN '과잉'
              WHEN COALESCE(s.current_stock,0) = m.avrqty_num THEN '적정'
              ELSE '부족'
            END
          ) = :status
        """;
      paramMap.addValue("status", status);
    }

    sql += " ORDER BY m.code ";

//    log.info("paramMap:{}", paramMap);
//    log.info("자재 적정재고(Avrqty) 현황 sql:{}", sql);
    return sqlRunner.getRows(sql, paramMap);
  }

}
