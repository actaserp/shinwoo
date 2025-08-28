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
        -- 0) 자재 마스터
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
        -- 1) 수주량(기간 조건은 헤더 기준)
        order_demand AS (
          SELECT
            li."Material_id" AS mat_id,
            SUM(CASE WHEN COALESCE(li."SujuQty2",0) > 0 THEN li."SujuQty2"
                     ELSE COALESCE(li."SujuQty",0) END)::numeric AS order_qty
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
        -- 3) 발주 총량(취소/강완 제외)
        po AS (
          SELECT
            b."Material_id" AS mat_id,
            SUM(COALESCE(b."SujuQty",0))::numeric AS po_qty
          FROM balju b
          JOIN balju_head bh ON bh.id = b."BaljuHead_id"
          WHERE bh.spjangcd = :spjangcd
            AND COALESCE(b."State",'draft') NOT IN ('canceled', 'force_completion')
          GROUP BY b."Material_id"
        ),
        -- 4) 발주 연계 입출고 집계
        --    입고: order_in / 출고(발주반품): order_return (현장 값에 맞게 수정)
        recv_ret AS (
          SELECT
            b.id            AS balju_line_id,
            b."Material_id" AS mat_id,
            SUM(
              CASE
                WHEN UPPER(COALESCE(TRIM(mi."InOut"),'')) = 'IN'
                 AND UPPER(COALESCE(TRIM(mi."InputType"),'')) = 'ORDER_IN'
                 AND COALESCE(mi."State",'') = 'confirmed'
                 AND mi."SourceTableName" = 'balju'
                 AND mi."SourceDataPk" = b.id
                THEN COALESCE(mi."InputQty",0)
                ELSE 0
              END
            )::numeric AS recv_qty,
            SUM(
              CASE
                WHEN UPPER(COALESCE(TRIM(mi."InOut"),'')) = 'OUT'
                 AND UPPER(COALESCE(TRIM(mi."OutputType"),'')) = 'ORDER_RETURN'
                 AND COALESCE(mi."State",'') = 'confirmed'
                 AND mi."SourceTableName" = 'balju'
                 AND mi."SourceDataPk" = b.id
                THEN COALESCE(mi."OutputQty",0)
                ELSE 0
              END
            )::numeric AS return_qty
          FROM balju b
          JOIN balju_head bh
            ON bh.id = b."BaljuHead_id"
           AND bh.spjangcd = :spjangcd
         LEFT JOIN mat_inout mi   
            ON mi."SourceTableName" = 'balju'
           AND mi."SourceDataPk" = b.id
           AND mi.spjangcd = :spjangcd
          WHERE UPPER(TRIM(COALESCE(b."State",'DRAFT'))) IN ('DRAFT','PARTIAL','RECEIVED')
          GROUP BY b.id, b."Material_id"
        ),
        -- 5) 자재별 ‘발주기준 실수령’ = 입고 - 반품
        recv_by_mat AS (
          SELECT
            r.mat_id,
            SUM(COALESCE(r.recv_qty,0) - COALESCE(r.return_qty,0))::numeric AS recv_effective
          FROM recv_ret r
          GROUP BY r.mat_id
        ),
        -- 6) 미입고(open) = max(발주총량 - 실수령, 0)
        incoming AS (
          SELECT
            COALESCE(p.mat_id, r.mat_id) AS mat_id,
            GREATEST(COALESCE(p.po_qty,0) - COALESCE(r.recv_effective,0), 0)::numeric AS incoming_qty
          FROM po p
          FULL JOIN recv_by_mat r ON r.mat_id = p.mat_id
        ),
        -- 7) 표시/판단 공통
        base AS (
          SELECT
            m.code                                  AS material_code,
            m.name                                  AS material_name,
            COALESCE(u."Name", m.packing_unit_name) AS unit_name,
            COALESCE(d.order_qty,0)                 AS order_qty,
            COALESCE(s.current_stock, 0)            AS current_stock,
            m.avrqty_num                            AS optimal_stock,
            COALESCE(i.incoming_qty, 0)             AS incoming_qty,
            -- 필요수량 계산: 수주 + 적정재고 - (현재고 + 미입고)
            (COALESCE(d.order_qty,0) + m.avrqty_num
              - (COALESCE(s.current_stock,0) + COALESCE(i.incoming_qty,0))) AS raw_gap
          FROM mat m
          LEFT JOIN stock s        ON s.mat_id = m.id
          LEFT JOIN order_demand d ON d.mat_id = m.id
          LEFT JOIN incoming i     ON i.mat_id = m.id
          LEFT JOIN "unit" u       ON u.id     = m."Unit_id"
          WHERE m.spjangcd = :spjangcd
        )
        SELECT
          material_code,
          material_name,
          unit_name,
          order_qty,
          incoming_qty,          -- 미입고수량(발주 오픈분)
          current_stock,
          optimal_stock,
          GREATEST(raw_gap, 0) AS need_more_qty,
          CASE
            WHEN raw_gap > 0 THEN '부족'
            WHEN raw_gap = 0 THEN '적정'
            ELSE '여유'
          END AS state
        FROM base
        where 1=1
      """;

    // 품명(키워드) 필터  ← 바깥 스코프 컬럼명 사용
    if (matName != null && !matName.isEmpty()) {
      sql += " AND material_name ILIKE :matName ";
      paramMap.addValue("matName", "%" + matName + "%");
    }

    // 상태 필터  ← raw_gap 기준으로 동일 CASE 재작성
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
        default: /* 사용자가 이미 '부족/적정/여유'를 넣은 경우 그대로 사용 */ break;
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
