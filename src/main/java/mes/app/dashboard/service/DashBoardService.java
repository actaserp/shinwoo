package mes.app.dashboard.service;

import io.micrometer.core.instrument.util.StringUtils;
import lombok.extern.slf4j.Slf4j;
import mes.app.sales.service.SujuService;
import mes.domain.entity.User;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Slf4j
@Service
public class DashBoardService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	SujuService sujuService;

	public List<Map<String, Object>> getOverview(Timestamp start, Timestamp end, String spjangcd, String choComp) {

		// 1) 각각 조회
//		List<Map<String, Object>> balju = getBaljuList(start, end, spjangcd, choComp);
		List<Map<String, Object>> suju  = getSujuList(start, end, spjangcd, choComp);
//		List<Map<String, Object>> invo  = getInvoList(start, end, spjangcd, choComp);
		List<Map<String, Object>> sales  = getSaleList(start, end, spjangcd, choComp);
//		List<Map<String, Object>> receive  = getReceiveList(start, end, spjangcd, choComp);
//		List<Map<String, Object>> payment  = getPaymentList(start, end, spjangcd, choComp);

		// 2) 구분(division) 부여 + 키 표준화(필요 시)
		/*balju.forEach(m -> {
			m.put("division", "발주");
			m.put("division_group", "매입");
			normalizeBaljuRow(m); // 아래 예시 참고
		});*/
		suju.forEach(m -> {
			m.put("division", "수주");
			m.put("division_group", "수주");
			normalizeSujuRow(m);  // 아래 예시 참고
		});
		/*invo.forEach(m -> {
			 m.put("division", "매입");
			 m.put("division_group", "매입");
			 normalizeInvoRow(m);
		 });
*/
		sales.forEach(m -> {
			m.put("division", "매출");
			m.put("division_group", "매출");
			normalizeSalesRow(m);
		});
		/*receive.forEach(m -> {
			m.put("division", "입금");
			m.put("division_group", "입금");
			normalizeReceiveRow(m);
		});
		payment.forEach(m -> {
			m.put("division", "출금");
			m.put("division_group", "출금");
			normalizePaymentRow(m);
		});*/

		// 3) 병합 + 정렬(납기일 우선, 없으면 주문일)
		List<Map<String, Object>> merged = new ArrayList<>( + sales.size() +
				suju.size() /*+ balju.size() + invo.size()  + receive.size() + payment.size()*/);
//		merged.addAll(balju);
		merged.addAll(suju);
//		merged.addAll(invo);
		merged.addAll(sales);
//		merged.addAll(receive);
//		merged.addAll(payment);

		// due_date, order_date 모두 'YYYY-MM-DD' 문자열
		Comparator<Map<String, Object>> byJumunDateDesc =
				Comparator.comparing(
						(Map<String, Object> m) -> Optional.ofNullable(m.get("JumunDate"))
								.map(Object::toString)
								.orElse(""),
						Comparator.nullsLast(String::compareTo)
				);

		merged.sort(byJumunDateDesc);

		return merged;
	}

	// 발주 행 표준화
	private void normalizeBaljuRow(Map<String, Object> m) {
		// 컬럼 리네이밍 예시 (이미 alias를 맞췄다면 생략 가능)
		m.putIfAbsent("head_id", m.remove("bh_id"));
		m.putIfAbsent("company_id", m.remove("Company_id"));
		m.putIfAbsent("company_name", m.remove("CompanyName"));
		m.putIfAbsent("type_name", m.remove("BaljuTypeName"));
		m.putIfAbsent("shipment_state_name", m.remove("ShipmentStateName"));
		m.putIfAbsent("product_name", m.remove("product_name"));
		m.putIfAbsent("price", m.remove("BaljuPrice"));
		m.putIfAbsent("vat", m.remove("BaljuVat"));
		m.putIfAbsent("total_price", m.remove("BaljuTotalPrice"));
		m.putIfAbsent("state_name", m.remove("bh_StateName"));
	}

	// 수주 행 표준화
	private void normalizeSujuRow(Map<String, Object> m) {
		m.putIfAbsent("head_id", m.remove("id"));
		m.putIfAbsent("company_id", m.remove("Company_id"));
		m.putIfAbsent("company_name", m.remove("CompanyName"));
		m.putIfAbsent("type_name", m.remove("SujuTypeName"));
		m.putIfAbsent("product_name", m.remove("product_name"));
		m.putIfAbsent("price", m.remove("sujuPrice"));
		m.putIfAbsent("vat", m.remove("sujuVat"));
		m.putIfAbsent("total_price", m.remove("TotalPrice"));
		// state_name: ShipmentStateName 우선, 없으면 StateName
		Object ship = m.get("ShipmentStateName");
		Object base = m.get("StateName");
		m.put("state_name", (ship != null && !ship.toString().trim().isEmpty()) ? ship : base);
		m.remove("ShipmentStateName");
		m.remove("StateName");

	}

	// 매입 행 표준화
	private void normalizeInvoRow(Map<String, Object> m) {
		m.putIfAbsent("head_id", m.remove("misnum"));
		m.putIfAbsent("company_id", m.remove("cltcd"));
		m.putIfAbsent("company_name", m.remove("cltnm"));
		//m.putIfAbsent("type_name", m.remove("SujuTypeName"));
		m.putIfAbsent("product_name", m.remove("item_summary"));
		m.putIfAbsent("price", m.remove("supplycost"));
		m.putIfAbsent("vat", m.remove("taxtotal"));
		m.putIfAbsent("total_price", m.remove("totalamt"));
		m.putIfAbsent("JumunDate", m.remove("misdate"));
		m.putIfAbsent("BusinessNumber", m.remove("BusinessNumber"));
		m.putIfAbsent("state_name", m.remove("misgubun_name"));
	}
	//매출 행 표준화
	private void normalizeSalesRow(Map<String, Object> m) {
		m.putIfAbsent("head_id", m.remove("misnum"));
		m.putIfAbsent("company_id", m.remove("cltcd"));
		m.putIfAbsent("company_name", m.remove("ivercorpnm"));
//		m.putIfAbsent("type_name", m.remove("taxtype"));
		m.putIfAbsent("state_name", m.remove("taxtype"));
		m.putIfAbsent("product_name", m.remove("item_summary"));
		m.putIfAbsent("price", m.remove("supplycost"));
		m.putIfAbsent("vat", m.remove("taxtotal"));
		m.putIfAbsent("total_price", m.remove("totalamt"));
		m.putIfAbsent("JumunDate", m.remove("misdate"));
		m.putIfAbsent("BusinessNumber", m.remove("ivercorpnum"));
	}

	//입금 행 표준화
	private void normalizeReceiveRow(Map<String, Object> m) {
		m.putIfAbsent("head_id", m.remove("head_id"));
		m.putIfAbsent("company_id", m.remove("cltcd"));
		m.putIfAbsent("company_name", m.remove("company_name"));
		m.putIfAbsent("state_name", m.remove("state_name"));
		m.putIfAbsent("price", m.remove("price"));
		m.putIfAbsent("vat", m.remove("vat"));
		m.putIfAbsent("total_price", m.remove("total_price"));
		m.putIfAbsent("JumunDate", m.remove("JumunDate"));

	}
	//출금 행 표준화
	private void normalizePaymentRow(Map<String, Object> m) {
		m.putIfAbsent("head_id", m.remove("head_id"));
		m.putIfAbsent("company_id", m.remove("cltcd"));
		m.putIfAbsent("company_name", m.remove("company_name"));
		m.putIfAbsent("state_name", m.remove("state_name"));
		m.putIfAbsent("price", m.remove("price"));
		m.putIfAbsent("vat", m.remove("vat"));
		m.putIfAbsent("total_price", m.remove("total_price"));
		m.putIfAbsent("JumunDate", m.remove("JumunDate"));
	}

	public List<Map<String, Object>> getBaljuList(Timestamp start, Timestamp end, String spjangcd, String choComp) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);

		String sql = """
        WITH base_data AS (
                   SELECT
                     bh.id AS bh_id,
                     bh."Company_id",
         b."CompanyName",
         c."BusinessNumber",
         b."BaljuHead_id",
         bh."JumunDate",
         bh."JumunNumber",
         mg."Name" AS "MaterialGroupName",
         fn_code_name('Balju_type', bh."SujuType") AS "BaljuTypeName",
         b.id AS balju_id,
         m."Code" AS product_code,
         m."Name" AS product_name,
         u."Name" AS unit,
         b."SujuQty",
         b."UnitPrice",
         b."Price",
         b."Vat",
         b."TotalAmount",
         fn_code_name('balju_state', bh."State") AS "StateName",
         mi."SujuQty2" AS "SujuQty2",
        GREATEST((b."SujuQty" - mi."SujuQty2"), 0) AS "SujuQty3",
         sh."Name" AS "ShipmentStateName",
         bh."DeliveryDate",
         bh."Description",
         (
          SELECT
            CASE
              WHEN COUNT(*) FILTER (
                WHERE
                  CASE
                    WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
        WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
        WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
        WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
        ELSE 'draft'
          END = 'received'
          ) = COUNT(*) THEN 'received'
          WHEN COUNT(*) FILTER (
            WHERE
              CASE
                WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
        WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
        WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
        WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
        ELSE 'draft'
          END = 'draft'
          ) = COUNT(*) THEN 'draft'
          WHEN COUNT(*) FILTER (
            WHERE
              CASE
                WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
        WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
        WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
        WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
        ELSE 'draft'
          END = 'canceled'
          ) = COUNT(*) THEN 'canceled'
          ELSE 'partial'
            END
          FROM balju b2
          LEFT JOIN (
            SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
        FROM mat_inout
        WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
        GROUP BY "SourceDataPk"
          ) mi2 ON mi2."SourceDataPk" = b2.id
          WHERE b2."BaljuHead_id" = bh.id
        ) AS "BalJuHeadType",
        fn_code_name(
          'balju_state',
          (
            SELECT
              CASE
                WHEN COUNT(*) FILTER (
                  WHERE
                    CASE
                      WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
          WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
          WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
          WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
          ELSE 'draft'
        END = 'received'
        ) = COUNT(*) THEN 'received'
        WHEN COUNT(*) FILTER (
          WHERE
            CASE
              WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
          WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
          WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
          WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
          ELSE 'draft'
        END = 'draft'
        ) = COUNT(*) THEN 'draft'
        WHEN COUNT(*) FILTER (
          WHERE
            CASE
              WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
          WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
          WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
          WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
          ELSE 'draft'
        END = 'canceled'
        ) = COUNT(*) THEN 'canceled'
        ELSE 'partial'
          END
        FROM balju b2
        LEFT JOIN (
          SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
          FROM mat_inout
          WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
          GROUP BY "SourceDataPk"
        ) mi2 ON mi2."SourceDataPk" = b2.id
        WHERE b2."BaljuHead_id" = bh.id
          )
          ) AS "bh_StateName",
           ROW_NUMBER() OVER (PARTITION BY bh."JumunNumber" ORDER BY b.id ASC) AS rn
             FROM balju_head bh
             LEFT JOIN balju b ON b."BaljuHead_id" = bh.id AND b.spjangcd = bh.spjangcd AND b."JumunNumber" = bh."JumunNumber"
             left join company c on bh."Company_id" = c.id
             INNER JOIN material m ON m.id = b."Material_id" AND m.spjangcd = b.spjangcd
             INNER JOIN mat_grp mg ON mg.id = m."MaterialGroup_id" AND mg.spjangcd = b.spjangcd
             LEFT JOIN unit u ON m."Unit_id" = u.id AND u.spjangcd = b.spjangcd
             LEFT JOIN store_house sh ON sh.id::varchar = b."ShipmentState" AND sh.spjangcd = b.spjangcd
             LEFT JOIN (
               SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
           FROM mat_inout
           WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
           GROUP BY "SourceDataPk"
             ) mi ON mi."SourceDataPk" = b.id
           WHERE bh.spjangcd = :spjangcd
           AND bh."JumunDate" BETWEEN :start AND :end
           and b."CompanyName" like :choComp
			)
			SELECT
			  bh_id,
			  "JumunNumber",
			  MAX("Company_id") AS "Company_id",
			  MAX("CompanyName") AS "CompanyName",
			  MAX("BusinessNumber") AS "BusinessNumber",
			  MAX("BaljuHead_id") AS "BaljuHead_id",
			  MAX("JumunDate") AS "JumunDate",
			  MAX("MaterialGroupName") AS "MaterialGroupName",
			  MAX("BaljuTypeName") AS "BaljuTypeName",
			  MAX(CASE WHEN rn = 1 THEN product_code END) AS product_code,
			  MAX(CASE WHEN rn = 1 THEN product_name END) AS product_name,
			  MAX(CASE WHEN rn = 1 THEN unit END) AS unit,
			  SUM("SujuQty") AS "SujuQty",
			  SUM("UnitPrice") AS "BaljuUnitPrice",
			  SUM("Price") AS "BaljuPrice",
			  SUM("Vat") AS "BaljuVat",
			 SUM("TotalAmount") AS "BaljuTotalPrice",
			  MAX("StateName") AS "StateName",
			  MAX("BalJuHeadType") AS "BalJuHeadType",
			  MAX("bh_StateName") AS "bh_StateName",
			  SUM("SujuQty2") AS "SujuQty2",
			  SUM(GREATEST("SujuQty" - COALESCE("SujuQty2", 0), 0)) AS "SujuQty3",
			  MAX("ShipmentStateName") AS "ShipmentStateName",
			  MAX("DeliveryDate") AS "DueDate",
			  MAX("Description") AS "Description"
			FROM base_data
			GROUP BY "JumunNumber", bh_id
			ORDER BY MAX("DeliveryDate") DESC, bh_id
        """;

		return this.sqlRunner.getRows(sql, dicParam);
	}

	public List<Map<String, Object>> getSujuList(Timestamp start, Timestamp end, String spjangcd, String choComp) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);

		String sql = """
				WITH suju_state_summary AS (
				  SELECT
				    sh.id AS suju_head_id,
				    CASE
				      WHEN BOOL_OR(s."State" = 'planned' AND jr."State" IS NOT NULL)
				        THEN MIN(jr."State")
				      ELSE MIN(s."State")
				    END AS summary_state,
				    BOOL_OR(s."State" = 'planned' AND jr."State" IS NOT NULL) AS used_job_state,
				    CASE
				      WHEN BOOL_OR(s."State" = 'planned' AND jr."State" IS NOT NULL)
				        THEN 'job_state'
				      ELSE 'suju_state'
				    END AS code_type
				  FROM suju_head sh
				  JOIN suju s ON s."SujuHead_id" = sh.id
				  LEFT JOIN (
				    SELECT DISTINCT ON ("SourceDataPk")
				           "SourceDataPk", "State"
				    FROM job_res
				    WHERE "SourceTableName" = 'suju'
				    ORDER BY "SourceDataPk", "_created" DESC
				  ) jr
				  ON jr."SourceDataPk" = s.id
				  GROUP BY sh.id
				),
				shipment_summary AS (
				  SELECT
				    s."SujuHead_id",
				    SUM(s."SujuQty") AS total_qty,
				    COALESCE(SUM(shp."shippedQty"), 0) AS total_shipped,
				    CASE
				      WHEN COUNT(shp."SourceDataPk") = 0 THEN ''
				      WHEN COALESCE(SUM(shp."shippedQty"), 0) = 0 THEN 'ordered'
				      WHEN SUM(shp."shippedQty") >= SUM(s."SujuQty") THEN 'shipped'
				      WHEN SUM(shp."shippedQty") < SUM(s."SujuQty") THEN 'partial'
				      ELSE ''
				    END AS shipment_state
				  FROM suju s
				  LEFT JOIN (
				    SELECT sh."SourceDataPk", SUM(sh."Qty") AS "shippedQty"
				    FROM shipment sh
				    JOIN shipment_head shh ON shh.id = sh."ShipmentHead_id"
				    WHERE shh.misnum IS NULL                -- 💡 미발행만 포함
				    GROUP BY sh."SourceDataPk"
				  ) shp ON shp."SourceDataPk" = s.id
				  WHERE NOT EXISTS (                        -- 💡 misnum 있는 출하건 연결된 수주는 제외
				      SELECT 1
				      FROM shipment s2
				      JOIN shipment_head shh2 ON shh2.id = s2."ShipmentHead_id"
				      WHERE s2."SourceDataPk" = s.id
				        AND shh2.misnum IS NOT NULL
				  )
				  GROUP BY s."SujuHead_id"
				)
				SELECT
				  sh.id,
				  sh."JumunNumber",
				  -- to_char(sh."JumunDate", 'yyyy-mm-dd') AS "JumunDate",
				  to_char(sh."DeliveryDate", 'yyyy-mm-dd') AS "JumunDate",
				  sh."Company_id",
				  c."BusinessNumber",
				  SUM(s."Price") AS "sujuPrice",
				  SUM(s."Vat") AS "sujuVat",
				  c."Name" AS "CompanyName",
				  sh."TotalPrice",
				  sh."Description",
				  sc_state."Value" AS "StateName",
				  sc_type."Value" AS "SujuTypeName",
				   
				  -- 대표 제품명 + 외 N개
				  CASE
					WHEN COUNT(DISTINCT s."Material_id") = 1 THEN MAX(m."Name")
					ELSE CONCAT(MAX(m."Name"), ' 외 ', COUNT(DISTINCT s."Material_id") - 1, '개')
				  END AS product_name,
				   
				  sss.summary_state AS "State",
				  sc_ship."Value" AS "ShipmentStateName"
				   
				FROM suju_head sh
				JOIN suju s ON s."SujuHead_id" = sh.id
				JOIN material m ON m.id = s."Material_id"
				LEFT JOIN (
				  SELECT sh."SourceDataPk", SUM(sh."Qty") AS "shippedQty"
				  FROM shipment sh
				  JOIN shipment_head shh ON shh.id = sh."ShipmentHead_id"
				  WHERE shh.misnum IS NULL          -- 💡 세금계산서 미발행건만 포함
				  GROUP BY sh."SourceDataPk"
				) shp ON shp."SourceDataPk" = s.id
				LEFT JOIN company c ON c.id = sh."Company_id"
				LEFT JOIN shipment_summary ss ON ss."SujuHead_id" = sh.id
				LEFT JOIN suju_state_summary sss ON sss.suju_head_id = sh.id
				LEFT JOIN sys_code sc_state
				           ON sc_state."Code" = sss.summary_state
				          AND sc_state."CodeType" = sss.code_type
				LEFT JOIN sys_code sc_type ON sc_type."Code" = sh."SujuType" AND sc_type."CodeType" = 'suju_type'
				LEFT JOIN sys_code sc_ship ON sc_ship."Code" = ss.shipment_state AND sc_ship."CodeType" = 'shipment_state'
					where 1 = 1
					and sh.spjangcd = :spjangcd
					and sh."DeliveryDate" between :start and :end
					and c."Name" like :choComp
					AND NOT EXISTS (
				       SELECT 1
				       FROM shipment s2
				       JOIN shipment_head shh2 ON shh2.id = s2."ShipmentHead_id"
				       WHERE s2."SourceDataPk" IN (
				           SELECT s3.id FROM suju s3 WHERE s3."SujuHead_id" = sh.id
				       )
				       AND shh2.misnum IS NOT NULL
				   )
					group by
						 sh.id,
						 sh."JumunNumber",
						 sh."DeliveryDate",
						 sh."DeliveryDate",
						 sh."Company_id",
						 c."Name",
						 c."BusinessNumber",
						 sh."TotalPrice",
						 sh."Description",
						 sh."SujuType",
						 sss.summary_state,
						 sc_state."Value",
						 sc_type."Value",
						 sc_ship."Value"
					order by sh."DeliveryDate" desc,  sh.id 
				""";

		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}

	//매입 리스트
	public List<Map<String, Object>> getInvoList(Timestamp start, Timestamp end, String spjangcd, String choComp){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);
		String sql = """
				WITH detail_summary AS (
				    SELECT
				        misnum,
				        MIN(itemnm) AS first_itemnm,
				        COUNT(*) AS item_count
				    FROM tb_invoicedetail
				    GROUP BY misnum
				),
				clt_unified AS (
				    SELECT id, '0' AS flag, "Code", "Name" AS name, NULL AS accnum, NULL AS accname, NULL AS cardnum, NULL AS cardnm FROM company
				UNION ALL
				SELECT id, '1' AS flag, "Code", "Name", NULL, NULL, NULL, NULL FROM person
				UNION ALL
				SELECT accid AS id, '2', NULL, NULL, accnum, accname, NULL, NULL FROM tb_account
				UNION ALL
				SELECT id, '3', NULL, NULL, NULL, null, cardnum, cardnm FROM tb_iz010
				),
				payclt_unified AS (
				    SELECT id, '0' AS flag, "Code", "Name" AS name, NULL AS accnum, NULL AS accname, NULL AS cardnum, NULL AS cardnm FROM company
				UNION ALL
				SELECT id, '1', "Code", "Name", NULL, NULL, NULL, NULL FROM person
				UNION ALL
				SELECT accid AS id, '2', NULL, NULL, accnum, accname, NULL, NULL FROM tb_account
				UNION ALL
				SELECT id, '3', NULL, NULL, NULL, null, cardnum, cardnm FROM tb_iz010
				)               
				SELECT
				    TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
				m.misnum,
				m.misgubun,
				purchase_type_code."Value" AS misgubun_name,
				m.paycltcd,
				m.cltcd,
				c."BusinessNumber"  ,
				COALESCE(cu.name, cu.accnum, cu.cardnum) AS cltnm,
				COALESCE(cu.accname, cu.cardnm, cu."Code") AS cltnmsub,
				COALESCE(pcu.name, pcu.accnum, pcu.cardnum) AS paycltnm,
				COALESCE(pcu.accname, pcu.cardnm, pcu."Code") AS paycltnmsub,
				m.totalamt,
				m.supplycost,
				m.taxtotal,
				m.title,
				m.deductioncd,
				de.name AS dedunm,
				m.depart_id,
				dp."Name" AS dpName,
				m.card_id,
				iz.cardnum AS incardnum,
				CASE
				    WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
				        WHEN ds.item_count = 1 THEN ds.first_itemnm
				        ELSE NULL
				    END AS item_summary             
				FROM tb_invoicement m
				left join company c on c.id = m.cltcd
				LEFT JOIN detail_summary ds ON m.misnum = ds.misnum
				LEFT JOIN clt_unified cu ON m.cltcd = cu.id AND m.cltflag = cu.flag
				LEFT JOIN payclt_unified pcu ON m.paycltcd = pcu.id AND m.paycltflag = pcu.flag
				LEFT JOIN vat_deduction_type de ON m.deductioncd = de.code
				LEFT JOIN depart dp ON m.depart_id = dp.id
				LEFT JOIN tb_iz010 iz ON m.card_id = iz.id
				LEFT JOIN sys_code purchase_type_code ON purchase_type_code."CodeType" = 'purchase_type'
				AND purchase_type_code."Code" = m.misgubun
				WHERE 1=1
				and m.spjangcd = :spjangcd
				and to_date(m.misdate, 'YYYYMMDD') between :start and :end
				AND (
				  :choComp IS NULL OR :choComp = '' OR :choComp = '%' OR
				  COALESCE(cu.name,'')      ILIKE :choComp OR
				  COALESCE(cu.accname,'')   ILIKE :choComp OR
				  COALESCE(cu."Code",'')    ILIKE :choComp OR
				  COALESCE(cu.accnum,'')    ILIKE :choComp OR
				  COALESCE(cu.cardnum,'')   ILIKE :choComp OR
				  COALESCE(pcu.name,'')     ILIKE :choComp OR
				  COALESCE(pcu.accname,'')  ILIKE :choComp OR
				  COALESCE(pcu."Code",'')   ILIKE :choComp OR
				  COALESCE(pcu.accnum,'')   ILIKE :choComp OR
				  COALESCE(pcu.cardnum,'')  ILIKE :choComp OR
				  COALESCE(c."BusinessNumber",'') ILIKE :choComp OR
				  CAST(m.misnum AS TEXT)    ILIKE :choComp OR
				  COALESCE(m.title,'')      ILIKE :choComp OR
				  COALESCE(de.name,'')      ILIKE :choComp OR
				  COALESCE(dp."Name",'')    ILIKE :choComp OR
				  COALESCE(iz.cardnum,'')   ILIKE :choComp OR
				  COALESCE(CASE
				    WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
				    WHEN ds.item_count = 1 THEN ds.first_itemnm
				    ELSE ''
				  END,'') ILIKE :choComp
				);
				""";
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}

	//매출 리스트
	public List<Map<String, Object>> getSaleList(Timestamp start, Timestamp end, String spjangcd, String choComp){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);
		String sql = """
				WITH detail_summary AS (
						 SELECT DISTINCT ON (misnum)
										misnum,
										itemnm AS first_itemnm,
										COUNT(*) OVER (PARTITION BY misnum) AS item_count
						 FROM tb_salesdetail
						 ORDER BY misnum, misseq
				 )
				 SELECT
						 TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
						 m.misnum,
						 m.misgubun,
						 sale_type_code."Value" AS misgubun_name,
						 m.cltcd,
						 m.ivercorpnum,
						 m.ivercorpnm,
						 m.totalamt,
						 m.supplycost,
						 m.taxtotal,
						 m.statecode,
						 state_code."Value" AS statecode_name,
						 TO_CHAR(TO_TIMESTAMP(m.statedt, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS') AS statedt_formatted,
						 m.iverceonm,
						 m.iveremail,
						 m.iveraddr,
						 m.taxtype,
						 issue_div."Value" AS issuediv_name,
						 m.issuediv,
						 m.modifycd,
						 CASE
							 WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
							 WHEN ds.item_count = 1 THEN ds.first_itemnm
							 ELSE NULL
						 END AS item_summary
				 FROM tb_salesment m
				 LEFT JOIN detail_summary ds
								ON m.misnum = ds.misnum
				 LEFT JOIN sys_code sale_type_code
								ON sale_type_code."CodeType" = 'sale_type'
							 AND sale_type_code."Code"     = m.misgubun
				 LEFT JOIN sys_code issue_div
								ON issue_div."CodeType" = 'issue_div'
							 AND issue_div."Code"     = m.issuediv
				 LEFT JOIN sys_code state_code
								ON state_code."CodeType" = 'state_code_pb'
							 AND state_code."Code"     = m.statecode::text
				 WHERE m.spjangcd = :spjangcd
					 AND TO_DATE(m.misdate, 'YYYYMMDD') BETWEEN :start AND :end
					 AND (
				         :choComp IS NULL OR :choComp = '' OR :choComp = '%' OR
				         m.ivercorpnm ILIKE :choComp OR
				         m.ivercorpnum ILIKE :choComp OR
				         CAST(m.cltcd AS TEXT) ILIKE :choComp OR
				         CAST(m.misnum AS TEXT) ILIKE :choComp OR
				         COALESCE(sale_type_code."Value",'') ILIKE :choComp OR
				         COALESCE(issue_div."Value",'')      ILIKE :choComp OR
				         COALESCE(state_code."Value",'')     ILIKE :choComp OR
				         COALESCE(
				           CASE
				             WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
				             WHEN ds.item_count = 1 THEN ds.first_itemnm
				             ELSE ''
				           END,''
				         ) ILIKE :choComp
				    )
				 ORDER BY m.misdate, m.misnum;
				""";
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}
	
	//입금 리스트
	public List<Map<String, Object>> getReceiveList(Timestamp start, Timestamp end, String spjangcd, String choComp){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);
		String sql = """
				SELECT
				    -- 숨김키
				    b.ioid AS head_id,
				    -- 구분(고정)
				    '입금' AS division,
				    -- 작성일자
				    TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
				    -- 상태(입금 유형명: deposit_type 값 우선, 없으면 거래명)
				    COALESCE(s."Value", t.tradenm) AS state_name,
				    b.cltcd,
				    -- 거래처명(클라이언트명)
				    COALESCE(
				        CASE
				            WHEN b.cltflag = '0' THEN c."Name"      -- 회사
				            WHEN b.cltflag = '1' THEN p."Name"      -- 개인
				            WHEN b.cltflag = '2' THEN d2.accname    -- 계좌명
				            WHEN b.cltflag = '3' THEN i.cardco      -- 카드사명
				        END, ''
				    ) AS company_name,
				    -- 사업자번호(또는 식별자 대체)
				    COALESCE(
				        CASE
				            WHEN b.cltflag = '0' THEN c."BusinessNumber" 
				            WHEN b.cltflag = '1' THEN p."Code"           
				            WHEN b.cltflag = '2' THEN d2.accnum         
				            WHEN b.cltflag = '3' THEN i.cardnum          
				        END, ''
				    ) AS "BusinessNumber",
				    
				    -- 공급가액: 입금 금액
				    COALESCE(b.accin, 0) AS price,
				    -- 세액: 수수료 매핑(필요 시 0으로 두고 fee를 별도 컬럼으로 쓰세요)
				    COALESCE(b.feeamt, 0) AS vat,
				    -- 합계: 입금 + 수수료
				    COALESCE(b.accin, 0) + COALESCE(b.feeamt, 0) AS total_price,
				    -- 메모
				    b.memo AS "Description"
				FROM tb_banktransit b
				LEFT JOIN tb_trade   t ON t.trid = b.trid
				LEFT JOIN sys_code   s ON s."Code" = b.iotype AND s."CodeType" = 'deposit_type'
				LEFT JOIN company    c ON c.id = b.cltcd
				LEFT JOIN person     p ON p.id = b.cltcd
				LEFT JOIN tb_account d2 ON d2.accid = b.cltcd
				LEFT JOIN tb_iz010   i  ON i.id   = b.cltcd
				WHERE b.ioflag = '0'                            -- 입금만
				  AND TO_DATE(b.trdate, 'YYYYMMDD')
				      BETWEEN :start AND :end
				  AND b.spjangcd = :spjangcd
				   AND (
				         :choComp IS NULL OR :choComp = '' OR :choComp = '%' OR
				         -- 이름/식별자
				         COALESCE(
				           CASE
				             WHEN b.cltflag = '0' THEN c."Name"
				             WHEN b.cltflag = '1' THEN p."Name"
				             WHEN b.cltflag = '2' THEN d2.accname
				             WHEN b.cltflag = '3' THEN i.cardco
				           END,''
				         ) ILIKE :choComp OR
				         COALESCE(
				           CASE
				             WHEN b.cltflag = '0' THEN c."BusinessNumber"
				             WHEN b.cltflag = '1' THEN p."Code"
				             WHEN b.cltflag = '2' THEN d2.accnum
				             WHEN b.cltflag = '3' THEN i.cardnum
				           END,''
				         ) ILIKE :choComp OR
				         -- 유형/은행/계좌/메모
				         COALESCE(s."Value",'')  ILIKE :choComp OR
				         COALESCE(t.tradenm,'')  ILIKE :choComp OR
				         COALESCE(b.banknm,'')   ILIKE :choComp OR
				         COALESCE(b.accnum,'')   ILIKE :choComp OR
				         COALESCE(b.memo,'')     ILIKE :choComp OR
				         COALESCE(b.remark1,'')  ILIKE :choComp
				    )
				ORDER BY b.trdate DESC, b.ioid desc;
				""";
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}
	
	//출금 리스트
	public List<Map<String, Object>> getPaymentList(Timestamp start, Timestamp end, String spjangcd, String choComp){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		String pattern = (choComp == null || choComp.isBlank()) ? "%" : "%" + choComp + "%";
		dicParam.addValue("choComp", pattern);
		String sql = """
				SELECT
				    -- 숨김키
				    b.ioid AS head_id,				   
				    '출금' AS division,
				    -- 작성일자
				    TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
				    COALESCE(s."Value", t.tradenm) AS state_name,
				    b.cltcd,
				    -- 거래처명(클라이언트명)
				    COALESCE(
				        CASE
				            WHEN b.cltflag = '0' THEN c."Name"      -- 회사
				            WHEN b.cltflag = '1' THEN p."Name"      -- 개인
				            WHEN b.cltflag = '2' THEN d2.accname    -- 계좌명
				            WHEN b.cltflag = '3' THEN i.cardco      -- 카드사명
				        END, ''
				    ) AS company_name,
				    -- 사업자번호(또는 식별자 대체)
				    COALESCE(
				        CASE
				            WHEN b.cltflag = '0' THEN c."BusinessNumber" -- 회사 사업자번호
				            WHEN b.cltflag = '1' THEN p."Code"           -- 개인 코드(있으면 주민/사번 등)
				            WHEN b.cltflag = '2' THEN d2.accnum          -- 계좌번호
				            WHEN b.cltflag = '3' THEN i.cardnum          -- 카드번호
				        END, ''
				    ) AS "BusinessNumber",
				    
				    -- 공급가액: 입금 금액
				    COALESCE(b.accout, 0) AS price,
				    -- 세액: 수수료 매핑(필요 시 0으로 두고 fee를 별도 컬럼으로 쓰세요)
				    COALESCE(b.feeamt, 0) AS vat,
				    -- 합계: 입금 + 수수료
				    COALESCE(b.accout, 0) + COALESCE(b.feeamt, 0) AS total_price,
				    -- 메모
				    b.memo AS "Description"
				FROM tb_banktransit b
				LEFT JOIN tb_trade   t ON t.trid = b.trid
				LEFT JOIN sys_code   s ON s."Code" = b.iotype AND s."CodeType" = 'deposit_type'
				LEFT JOIN company    c ON c.id = b.cltcd
				LEFT JOIN person     p ON p.id = b.cltcd
				LEFT JOIN tb_account d2 ON d2.accid = b.cltcd
				LEFT JOIN tb_iz010   i  ON i.id   = b.cltcd
				WHERE b.ioflag = '1'                            --출금만
				  AND TO_DATE(b.trdate, 'YYYYMMDD')
				      BETWEEN :start AND :end
				  AND b.spjangcd = :spjangcd
				  AND (
				         :choComp IS NULL OR :choComp = '' OR :choComp = '%' OR
				         COALESCE(
				           CASE
				             WHEN b.cltflag = '0' THEN c."Name"
				             WHEN b.cltflag = '1' THEN p."Name"
				             WHEN b.cltflag = '2' THEN d2.accname
				             WHEN b.cltflag = '3' THEN i.cardco
				           END,''
				         ) ILIKE :choComp OR
				         COALESCE(
				           CASE
				             WHEN b.cltflag = '0' THEN c."BusinessNumber"
				             WHEN b.cltflag = '1' THEN p."Code"
				             WHEN b.cltflag = '2' THEN d2.accnum
				             WHEN b.cltflag = '3' THEN i.cardnum
				           END,''
				         ) ILIKE :choComp OR
				         COALESCE(s."Value",'')  ILIKE :choComp OR
				         COALESCE(t.tradenm,'')  ILIKE :choComp OR
				         COALESCE(b.banknm,'')   ILIKE :choComp OR
				         COALESCE(b.accnum,'')   ILIKE :choComp OR
				         COALESCE(b.memo,'')     ILIKE :choComp OR
				         COALESCE(b.remark1,'')  ILIKE :choComp
				    )
				ORDER BY b.trdate DESC, b.ioid desc;
				""";
		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);

		return itmes;
	}

	// 수주 디테일
	public List<Map<String, Object>> getSujuDetail(int id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String detailSql = """ 
				WITH shipment_status AS (
				  SELECT "SourceDataPk", SUM("Qty") AS shipped_qty
				  FROM shipment
				  WHERE "SourceTableName" = 'rela_data'
				  GROUP BY "SourceDataPk"
				),
				suju_with_state AS (
				  SELECT
				    s.id,
				    s."SujuHead_id" AS head_id,
				    s."Material_id",
				    m."Code" AS "product_code",
				    s."JumunDate",
				    m."Name" AS "product_name",
				    u."Name" AS "unit",
				    s."SujuQty"     AS "quantity",          -- 헤더 원수량
				    s."UnitPrice"   AS "unit_price",
				    s."Vat"         AS "vat_amount",        -- 헤더 금액(원본)
				    s."Price"       AS "supply_amount",
				    s."TotalAmount" AS "total_amount",
				    s."Description" AS "description",
				    s."State"       AS "original_state",
				    COALESCE(sh.shipped_qty, -1) AS "shipped_qty",
				    s."Standard"    AS standard,            -- 헤더 규격(원본)
				    CASE
				      WHEN sh.shipped_qty = -1 THEN s."State"
				      WHEN sh.shipped_qty = 0  THEN 'force_completion'
				      WHEN sh.shipped_qty >= s."SujuQty" THEN 'shipped'
				      WHEN sh.shipped_qty <  s."SujuQty" THEN 'partial'
				      ELSE s."State"
				    END AS final_state
				  FROM suju s
				  JOIN material m       ON m.id = s."Material_id"
				  JOIN mat_grp  mg      ON mg.id = m."MaterialGroup_id"
				  LEFT JOIN unit u      ON m."Unit_id" = u.id
				  LEFT JOIN TB_DA003 p  ON p."projno" = s.project_id
				  LEFT JOIN shipment_status sh ON sh."SourceDataPk" = s.id
				  WHERE s."SujuHead_id" = :id
				),
				-- ▼ 부모 라벨용: 첫 규격 + ' 외 N개'
				first_std AS (
				  SELECT
				    s.id,
				    (SELECT sd."Standard"
				       FROM suju_detail sd
				      WHERE sd."suju_id" = s.id
				        AND COALESCE(sd."Standard",'') <> ''
				      ORDER BY sd.id
				      LIMIT 1) AS first_standard,
				    (SELECT GREATEST(COUNT(*) - 1, 0)
				       FROM suju_detail sd
				      WHERE sd."suju_id" = s.id
				        AND COALESCE(sd."Standard",'') <> '') AS rest_count
				  FROM suju_with_state s
				),
				base AS (
				  SELECT
				    s.*,
				    -- 상세/폴백 조인
				    x.detail_id,
				    x.line_standard,
				    x.line_qty,
				    x.is_fallback,
				    -- id별 첫 행 판별
				    ROW_NUMBER() OVER (
				      PARTITION BY s.id
				      ORDER BY x.detail_id NULLS LAST, x.is_fallback DESC
				    ) AS rn
				  FROM suju_with_state s
				  LEFT JOIN LATERAL (
				    SELECT sd."id" AS detail_id, sd."Standard" AS line_standard, sd."Qty" AS line_qty, FALSE AS is_fallback
				      FROM suju_detail sd WHERE sd."suju_id" = s.id
				    UNION ALL
				    SELECT NULL::int, s.standard, s."quantity", TRUE
				      WHERE NOT EXISTS (SELECT 1 FROM suju_detail sd2 WHERE sd2."suju_id" = s.id)
				  ) x ON TRUE
				)
				SELECT
					b.id, b.head_id, b."Material_id", b."product_code", b."JumunDate",
					b."product_name", b."unit",
					b.line_standard AS standard,
					b.line_qty      AS quantity,
					-- ★ 자식 금액은 전부 NULL (재계산 트리거 방지)
					NULL::numeric   AS unit_price,
					NULL::numeric   AS supply_amount,
					NULL::numeric   AS vat_amount,
					NULL::numeric   AS total_amount,
					-- ★ 헤더 금액은 별도 컬럼으로 항상 실어보냄
					b."unit_price"     AS header_unit_price,
					b."supply_amount"  AS header_supply_amount,
					b."vat_amount"     AS header_vat_amount,
					b."total_amount"   AS header_total_amount,
					b."quantity"       AS header_quantity,
					-- ▼ 라벨: DB 값 그대로
					b.standard AS header_standard_label,				
					b.final_state AS "state",
					COALESCE(sc_ship."Value", sc_suju."Value") AS "state_name",
					CASE WHEN b.rn = 1 THEN b."description" END AS "description",
					b.detail_id, b.is_fallback
				FROM base b
				-- LEFT JOIN first_std fs ON fs.id = b.id  -- 삭제
				LEFT JOIN sys_code sc_ship
					ON sc_ship."Code" = b.final_state AND sc_ship."CodeType" = 'shipment_state'
				LEFT JOIN sys_code sc_suju
					ON sc_suju."Code" = b.final_state AND sc_suju."CodeType" = 'suju_state'
				ORDER BY b.id, b.detail_id NULLS LAST;
		""";

        return this.sqlRunner.getRows(detailSql, paramMap);
	}

	// 발주 디테일
	public List<Map<String, Object>> getBaljuDetail(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String sql = """
			WITH balju_total AS (
			  SELECT "BaljuHead_id" AS bh_id,
					 SUM(COALESCE("TotalAmount", 0)) AS total_amount_sum
			  FROM balju
			  GROUP BY "BaljuHead_id"
			)
			SELECT
			  bh.id,
			  bh."JumunDate",
			  b.id,
			  b."Material_id",
			  COALESCE(m."Code", '') AS product_code,
			  COALESCE(m."Name", '') AS product_name,
			  b."SujuQty" as quantity,
			  b."UnitPrice" AS "unit_price",
			  b."Price" AS "supply_amount",
			  b."Vat" AS "vat_amount",
			  b."TotalAmount" AS "total_amount",
			  COALESCE(bt.total_amount_sum, 0) AS "BaljuTotalPrice",
			  b."Description" as description,
			  m."Standard1" as standard,
			  (
				SELECT
				  CASE
					WHEN COUNT(*) FILTER (WHERE
					  CASE
						WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						ELSE 'draft'
					  END = 'received'
					) = COUNT(*) THEN 'received'
					WHEN COUNT(*) FILTER (WHERE
					  CASE
						WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						ELSE 'draft'
					  END = 'draft'
					) = COUNT(*) THEN 'draft'
					WHEN COUNT(*) FILTER (WHERE
					  CASE
						WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						ELSE 'draft'
					  END = 'canceled'
					) = COUNT(*) THEN 'canceled'
					ELSE 'partial'
				  END
				FROM balju b2
				LEFT JOIN (
				  SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
				  FROM mat_inout
				  WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
				  GROUP BY "SourceDataPk"
				) mi2 ON mi2."SourceDataPk" = b2.id
				WHERE b2."BaljuHead_id" = bh.id
			  ) AS "BalJuHeadType",
			  fn_code_name(
				'balju_state',
				(
				  SELECT
					CASE
					  WHEN COUNT(*) FILTER (WHERE
						CASE
						  WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						  WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						  WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						  WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						  ELSE 'draft'
						END = 'received'
					  ) = COUNT(*) THEN 'received'
					  WHEN COUNT(*) FILTER (WHERE
						CASE
						  WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						  WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						  WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						  WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						  ELSE 'draft'
						END = 'draft'
					  ) = COUNT(*) THEN 'draft'
					  WHEN COUNT(*) FILTER (WHERE
						CASE
						  WHEN b2."State" IN ('canceled', 'force_completion') THEN b2."State"
						  WHEN COALESCE(mi2."SujuQty2", 0) = 0 AND b2."SujuQty" > 0 THEN 'draft'
						  WHEN COALESCE(mi2."SujuQty2", 0) >= b2."SujuQty" THEN 'received'
						  WHEN COALESCE(mi2."SujuQty2", 0) > 0 AND COALESCE(mi2."SujuQty2", 0) < b2."SujuQty" THEN 'partial'
						  ELSE 'draft'
						END = 'canceled'
					  ) = COUNT(*) THEN 'canceled'
					  ELSE 'partial'
					END
				  FROM balju b2
				  LEFT JOIN (
					SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
					FROM mat_inout
					WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
					GROUP BY "SourceDataPk"
				  ) mi2 ON mi2."SourceDataPk" = b2.id
				  WHERE b2."BaljuHead_id" = bh.id
				)
			  ) AS "bh_StateName",
			  -- 개별 balju 상태
			  CASE
				WHEN b."State" IN ('canceled', 'force_completion') THEN b."State"
				WHEN COALESCE(mi."SujuQty2", 0) = 0 AND b."SujuQty" > 0 THEN 'draft'
				WHEN COALESCE(mi."SujuQty2", 0) >= b."SujuQty" THEN 'received'
				WHEN COALESCE(mi."SujuQty2", 0) > 0 AND COALESCE(mi."SujuQty2", 0) < b."SujuQty" THEN 'partial'
				ELSE 'draft'
			  END AS "BalJuType",
			  -- 코드 이름 매핑(라인)
			  fn_code_name(
				'balju_state',
				CASE
				  WHEN b."State" IN ('canceled', 'force_completion') THEN b."State"
				  WHEN COALESCE(mi."SujuQty2", 0) = 0 AND b."SujuQty" > 0 THEN 'draft'
				  WHEN COALESCE(mi."SujuQty2", 0) >= b."SujuQty" THEN 'received'
				  WHEN COALESCE(mi."SujuQty2", 0) > 0 AND COALESCE(mi."SujuQty2", 0) < b."SujuQty" THEN 'partial'
				  ELSE 'draft'
				END
			  ) AS "state_name"
			FROM balju_head bh
			LEFT JOIN balju b ON b."BaljuHead_id" = bh.id
			LEFT JOIN material m ON m.id = b."Material_id" AND m.spjangcd = b.spjangcd
			LEFT JOIN mat_grp mg ON mg.id = m."MaterialGroup_id" AND mg.spjangcd = b.spjangcd
			LEFT JOIN unit u ON m."Unit_id" = u.id AND u.spjangcd = b.spjangcd
			LEFT JOIN company c ON c.id = b."Company_id"
			LEFT JOIN sys_code s ON bh."SujuType" = s."Code" AND s."CodeType" = 'Balju_type'
			LEFT JOIN (
			  SELECT "SourceDataPk", SUM("InputQty") AS "SujuQty2"
			  FROM mat_inout
			  WHERE "SourceTableName" = 'balju' AND COALESCE("_status", 'a') = 'a'
			  GROUP BY "SourceDataPk"
			) mi ON mi."SourceDataPk" = b.id
			LEFT JOIN balju_total bt ON bt.bh_id = bh.id
			WHERE bh.id = :id
        """;

        return sqlRunner.getRows(sql, paramMap);
	}

	//매입 디테일
	public List<Map<String, Object>> getInvoDetail(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("misnum", id);
		String sql = """ 
			select
				TO_CHAR(TO_DATE(d.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
				purchase_type_code."Value" AS state_name,
				d.itemnm as product_name,
				d.spec as standard,
				d.qty as quantity,
				d.unitcost as unit_price,
				d.supplycost as supply_amount ,
				d.taxtotal as vat_amount,
				(d.supplycost + d.taxtotal) AS total_amount,
				d.remark as description
			 from tb_invoicedetail d
			 left join tb_invoicement m ON m.misnum = d.misnum 
			 LEFT JOIN sys_code purchase_type_code ON purchase_type_code."CodeType" = 'purchase_type' 
			 AND purchase_type_code."Code" = m.misgubun
				WHERE d.misnum = :misnum
				ORDER BY d.misseq::int asc;
                 """;
		return sqlRunner.getRows(sql, paramMap);
	}

	//매출
	public List<Map<String, Object>> getSalesDetail(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("misnum", id);
		String sql= """
				select
				 TO_CHAR(TO_DATE(d.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
				 h.taxtype as state_name,
				 d.itemnm as product_name,
				 d.spec as standard,
				 d.qty as quantity,
				 d.unitcost as unit_price,
				 d.supplycost as supply_amount ,
				 d.taxtotal as vat_amount,
				 (d.supplycost + d.taxtotal) AS total_amount,
				 d.remark as description
				from tb_salesdetail d
				LEFT JOIN tb_salesment h
				 ON h.misnum = d.misnum 
				AND h.misdate = d.misdate
				 WHERE d.misnum = :misnum
				 ORDER BY d.misseq::int asc;
				""";
		return sqlRunner.getRows(sql, paramMap);
	}
	
	//입금
//	public List<Map<String, Object>> getReceiveDetail(int id) {
//		MapSqlParameterSource paramMap = new MapSqlParameterSource();
//		paramMap.addValue("ioid", id);
//		String sql= """
//				SELECT
//				    b.ioid AS id,
//				    -- 구분(고정)
//				    '입금' AS division,
//				    TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
//				    COALESCE(s."Value", t.tradenm) AS state_name,
//				    COALESCE(b.accin, 0) AS supply_amount,
//				    COALESCE(b.feeamt, 0) AS vat_amount,
//				    COALESCE(b.accin, 0) + COALESCE(b.feeamt, 0) AS total_amount,
//				    b.remark1 AS "description"
//				FROM tb_banktransit b
//				LEFT JOIN tb_trade t
//				       ON t.trid = b.trid
//				LEFT JOIN sys_code s
//				       ON s."Code" = b.iotype
//				      AND s."CodeType" = 'deposit_type'
//				WHERE b.ioflag = '0'  -- 입금만
//				  AND b.ioid  = :ioid
//				ORDER BY b.trdate DESC, b.ioid DESC;
//				""";
//		log.info("입금 상세 read SQL: {}", sql);
//    log.info("SQL Parameters: {}", paramMap.getValues());
//		return sqlRunner.getRows(sql, paramMap);
//	}
	public List<Map<String, Object>> getDepositTotalList(String start_date, String end_date, Integer company, String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		DateTimeFormatter dbFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

		LocalDate startDate = LocalDate.parse(start_date, inputFormatter);
		LocalDate endDate = LocalDate.parse(end_date, inputFormatter);

		String formattedStart = startDate.format(dbFormatter);
		String formattedEnd = endDate.format(dbFormatter);

		paramMap.addValue("start", formattedStart);
		paramMap.addValue("end", formattedEnd);
		paramMap.addValue("spjangcd", spjangcd);

		if (company != null) {
			paramMap.addValue("company", company);
		}

		StringBuilder sql = new StringBuilder();

		sql.append("""
        WITH sales_data AS (
            SELECT 
                TO_CHAR(TO_DATE(s.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS date,
                COUNT(*) AS sales_count,
                SUM(s.totalamt) AS sales
            FROM tb_salesment s
            WHERE s.misdate BETWEEN :start AND :end
              AND s.spjangcd = :spjangcd
        """);

		if (company != null) {
			sql.append(" AND s.cltcd = :company ");
		}

		sql.append("""
            GROUP BY TO_CHAR(TO_DATE(s.misdate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ),
        accin_data AS (
            SELECT 
                TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS date,
                COUNT(*) AS accin_count,
                SUM(b.accin) AS accin
            FROM tb_banktransit b
            WHERE b.trdate BETWEEN :start AND :end
              AND b.ioflag = '0'
              AND b.spjangcd = :spjangcd
        """);

		if (company != null) {
			sql.append(" AND b.cltcd = :company ");
		}

		sql.append("""
            GROUP BY TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD')
        )
        SELECT
            COALESCE(s.date, a.date) AS date,
            COALESCE(s.sales_count, 0) AS sales_count,
            COALESCE(s.sales, 0) AS sales,
            COALESCE(a.accin_count, 0) AS accin_count,
            COALESCE(a.accin, 0) AS accin
        FROM sales_data s
        FULL OUTER JOIN accin_data a
        ON s.date = a.date
        ORDER BY date
    """);

		return this.sqlRunner.getRows(sql.toString(), paramMap);
	}


	// 업체별 종합 미수/미지급 현황 (마감 이후 ~ 조회일)
	public List<Map<String, Object>> getDetailFinanceTotal(Integer company, String spjangcd, String endDate) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		DateTimeFormatter dbFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

		LocalDate end_date = LocalDate.parse(endDate, inputFormatter);
		String formattedEnd = end_date.format(dbFormatter);
		String baseYmStr = YearMonth.from(end_date).format(DateTimeFormatter.ofPattern("yyyyMM"));

		paramMap.addValue("end", formattedEnd);
		paramMap.addValue("baseYm", baseYmStr);
		paramMap.addValue("spjangcd", spjangcd);
		if (company != null) {
			paramMap.addValue("company", company);
		}

		StringBuilder sql = new StringBuilder();

		sql.append("""
    WITH lastym AS (
        SELECT cltcd, MAX(yyyymm) AS yyyymm
        FROM tb_yearamt
        WHERE yyyymm < :baseYm
          AND spjangcd = :spjangcd
        GROUP BY cltcd
    ),
    base_dates AS (
        SELECT 
            COALESCE(
                (SELECT TO_CHAR(TO_DATE(MAX(yyyymm), 'YYYYMM') + interval '1 month', 'YYYYMMDD')
                 FROM lastym),
                TO_CHAR(TO_DATE(:baseYm || '01', 'YYYYMMDD') - interval '1 month', 'YYYYMMDD')
            ) AS next_start
    ),
    last_amt AS (
        SELECT y.cltcd, SUM(y.yearamt) AS yearamt
        FROM tb_yearamt y
        JOIN lastym m ON y.cltcd = m.cltcd AND y.yyyymm = m.yyyymm
        WHERE y.spjangcd = :spjangcd
        GROUP BY y.cltcd
    ),
    final_prev_amt AS (
        SELECT y.cltcd, y.yearamt AS prev_amt, (SELECT next_start FROM base_dates) AS next_start
        FROM last_amt y
    ),
    sales_amt AS (
        SELECT c.id AS cltcd, SUM(COALESCE(s.totalamt, 0)) AS sales
        FROM company c
        LEFT JOIN tb_salesment s
            ON c.id = s.cltcd
            AND s.misdate BETWEEN (SELECT next_start FROM base_dates) AND :end
            AND s.spjangcd = :spjangcd
        GROUP BY c.id
    ),
    purchase_amt AS (
        SELECT c.id AS cltcd, SUM(COALESCE(p.totalamt, 0)) AS purchase
        FROM company c
        LEFT JOIN tb_invoicement p
            ON c.id = p.cltcd
            AND p.misdate BETWEEN (SELECT next_start FROM base_dates) AND :end
            AND p.spjangcd = :spjangcd
        GROUP BY c.id
    ),
    accin_amt AS (
        SELECT c.id AS cltcd, SUM(COALESCE(b.accin, 0)) AS accin
        FROM company c
        LEFT JOIN tb_banktransit b
            ON c.id = b.cltcd
            AND b.trdate BETWEEN (SELECT next_start FROM base_dates) AND :end
            AND b.ioflag = '0'
            AND b.spjangcd = :spjangcd
        GROUP BY c.id
    ),
    accout_amt AS (
        SELECT c.id AS cltcd, SUM(COALESCE(b.accout, 0)) AS accout
        FROM company c
        LEFT JOIN tb_banktransit b
            ON c.id = b.cltcd
            AND b.trdate BETWEEN (SELECT next_start FROM base_dates) AND :end
            AND b.ioflag = '1'
            AND b.spjangcd = :spjangcd
        GROUP BY c.id
    )
    SELECT
        m.id AS cltcd,                                  -- 거래처코드
        m."Name" AS clt_name,                           -- 거래처명

        -- 📌 ① 전월이월금액 (마감 기준)
        TO_CHAR(COALESCE(f.prev_amt, 0), 'FM999,999,999,999') AS prev_amt,

        -- 📌 ② 매출금액 (tb_salesment)
        TO_CHAR(COALESCE(s.sales, 0), 'FM999,999,999,999') AS sales,

        -- 📌 ③ 매입금액 (tb_invoicement)
        TO_CHAR(COALESCE(p.purchase, 0), 'FM999,999,999,999') AS purchase,

        -- 📌 ④ 입금금액 (tb_banktransit, ioflag='0')
        TO_CHAR(COALESCE(ai.accin, 0), 'FM999,999,999,999') AS accin,

        -- 📌 ⑤ 지급금액 (tb_banktransit, ioflag='1')
        TO_CHAR(COALESCE(ao.accout, 0), 'FM999,999,999,999') AS accout,

        -- 📘 ⑥ 미수총액 = 매출 - 입금 (아직 받지 못한 금액)
        TO_CHAR(
            COALESCE(s.sales, 0) - COALESCE(ai.accin, 0),
            'FM999,999,999,999'
        ) AS misu_total,

        -- 📙 ⑦ 미지총액 = 매입 - 지급 (아직 지급하지 않은 금액)
        TO_CHAR(
            COALESCE(p.purchase, 0) - COALESCE(ao.accout, 0),
            'FM999,999,999,999'
        ) AS miji_total,

        -- 📗 ⑧ 상계잔액 = 전월 + (매출 - 입금) + (매입 - 지급)
        --     → 미수 + 미지급 모두 반영된 최종 잔액
        TO_CHAR(
            (COALESCE(f.prev_amt, 0)
             + (COALESCE(s.sales, 0) - COALESCE(ai.accin, 0))
             + (COALESCE(p.purchase, 0) - COALESCE(ao.accout, 0))),
            'FM999,999,999,999'
        ) AS balance

    FROM company m
    LEFT JOIN final_prev_amt f ON m.id = f.cltcd
    LEFT JOIN sales_amt s ON m.id = s.cltcd
    LEFT JOIN purchase_amt p ON m.id = p.cltcd
    LEFT JOIN accin_amt ai ON m.id = ai.cltcd
    LEFT JOIN accout_amt ao ON m.id = ao.cltcd
    WHERE m.id = :company
    ORDER BY m."Name"
    """);

		List<Map<String, Object>> result = this.sqlRunner.getRows(sql.toString(), paramMap);
		return result != null ? result : new ArrayList<>();
	}






	//출금
//	public List<Map<String, Object>> getPaymentDetail(int id) {
//		MapSqlParameterSource paramMap = new MapSqlParameterSource();
//		paramMap.addValue("ioid", id);
//		String sql= """
//				SELECT
//				    b.ioid AS id,
//				    -- 구분(고정)
//				    '출금' AS division,
//				    TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "JumunDate",
//				    COALESCE(s."Value", t.tradenm) AS state_name,
//				    COALESCE(b.accout, 0) AS supply_amount,                 -- 출금 금액
//				    COALESCE(b.feeamt, 0) AS vat_amount,                    -- 수수료(필요 시 0으로)
//				    COALESCE(b.accout, 0) + COALESCE(b.feeamt, 0) AS total_amount,
//				    b.remark1 AS "description"
//				FROM tb_banktransit b
//				LEFT JOIN tb_trade t
//				       ON t.trid = b.trid
//				LEFT JOIN sys_code s
//				       ON s."Code" = b.iotype
//				      AND s."CodeType" = 'deposit_type'   -- 출금 유형 코드타입이 따로 있으면 여기 변경
//				WHERE b.ioflag = '1'  -- 출금만
//				  AND b.ioid  = :ioid
//				ORDER BY b.trdate DESC, b.ioid DESC;
//				""";
//		return sqlRunner.getRows(sql, paramMap);
//	}
	public List<Map<String, Object>> getPayableTotalList(String start_date, String end_date, Integer company, String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		DateTimeFormatter dbFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

		LocalDate startDate = LocalDate.parse(start_date, inputFormatter);
		LocalDate endDate = LocalDate.parse(end_date, inputFormatter);

		String formattedStart = startDate.format(dbFormatter);
		String formattedEnd = endDate.format(dbFormatter);

		paramMap.addValue("start", formattedStart);
		paramMap.addValue("end", formattedEnd);
		paramMap.addValue("spjangcd", spjangcd);

		if (company != null) {
			paramMap.addValue("company", company);
		}

		StringBuilder sql = new StringBuilder();

		sql.append("""
        WITH purchase_data AS (
            SELECT 
                TO_CHAR(TO_DATE(i.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS date,
                COUNT(*) AS purchase_count,
                SUM(i.totalamt) AS purchase
            FROM tb_invoicement i
            WHERE i.misdate BETWEEN :start AND :end
              AND i.spjangcd = :spjangcd
    """);

		if (company != null) {
			sql.append(" AND i.cltcd = :company ");
		}

		sql.append("""
            GROUP BY TO_CHAR(TO_DATE(i.misdate, 'YYYYMMDD'), 'YYYY-MM-DD')
        ),
        payment_data AS (
            SELECT 
                TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS date,
                COUNT(*) AS payment_count,
                SUM(b.accout) AS payment
            FROM tb_banktransit b
            WHERE b.trdate BETWEEN :start AND :end
              AND b.ioflag = '1'
              AND b.spjangcd = :spjangcd
    """);

		if (company != null) {
			sql.append(" AND b.cltcd = :company ");
		}

		sql.append("""
            GROUP BY TO_CHAR(TO_DATE(b.trdate, 'YYYYMMDD'), 'YYYY-MM-DD')
        )
        SELECT
            COALESCE(p.date, b.date) AS date,
            COALESCE(p.purchase_count, 0) AS purchase_count,
            TO_CHAR(COALESCE(p.purchase, 0), 'FM999,999,999,999') AS purchase,
            COALESCE(b.payment_count, 0) AS payment_count,
            TO_CHAR(COALESCE(b.payment, 0), 'FM999,999,999,999') AS payment,
            TO_CHAR(COALESCE(p.purchase, 0) - COALESCE(b.payment, 0), 'FM999,999,999,999') AS unpaid_balance
        FROM purchase_data p
        FULL OUTER JOIN payment_data b
        ON p.date = b.date
        ORDER BY date
    """);

		return this.sqlRunner.getRows(sql.toString(), paramMap);
	}


	// 수주 이력
	public List<Map<String, Object>> getSujuHistory(int id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String detailSql = """ 
			WITH params AS (SELECT :id::int AS head_id),
			   suju_lines AS (
				 SELECT
				   s.id            AS suju_id,
				   s."Material_id",
				   m."Code"        AS product_code,
				   m."Name"        AS product_name,
				   s."Standard"    AS standard
				 FROM suju s
				 JOIN params p ON s."SujuHead_id" = p.head_id
				 LEFT JOIN material m ON m.id = s."Material_id"
			   ),
			   ev AS (
				 -- 수주(각 라인 1건)
				 SELECT
				   'suju'            AS event_type,
				   s.id              AS event_pk,
				   s."_created"      AS event_time,
				   s."_creater_id"   AS actor_id,
				   s."State"         AS state,
				   s."JumunDate"     AS biz_date,
				   sl.suju_id,
				   sl.product_code,
				   sl.product_name,
				   sl.standard,
				   'suju_state'::text AS code_type           -- ← 매핑용 추가
				 FROM suju_lines sl
				 JOIN suju s ON s.id = sl.suju_id
			   
				 UNION ALL
			   
				 -- 생산(여러 건)
				 SELECT
				   'job'             AS event_type,
				   j.id              AS event_pk,
				   j."_created"      AS event_time,
				   j."_creater_id"   AS actor_id,
				   j."State"         AS state,
				   j."ProductionDate"AS biz_date,
				   sl.suju_id,
				   sl.product_code,
				   sl.product_name,
				   sl.standard,
				   'job_state'::text AS code_type            -- ← 매핑용 추가
				 FROM suju_lines sl
				 JOIN job_res j
				   ON j."SourceTableName" = 'suju'
				  AND j."SourceDataPk"    = sl.suju_id
			   
				 UNION ALL
			   
				 -- 출고(여러 건)
				 SELECT
				   'shipment'         AS event_type,
				   shh.id             AS event_pk,
				   shh."_created"     AS event_time,
				   shh."_creater_id"  AS actor_id,
				   shh."State"        AS state,
				   shh."ShipDate"     AS biz_date,
				   sl.suju_id,
				   sl.product_code,
				   sl.product_name,
				   sl.standard,
				   'shipment_state'::text AS code_type       -- ← 매핑용 추가
				 FROM suju_lines sl
				 JOIN shipment sh
				   ON sh."SourceTableName" = 'rela_data'
				  AND sh."SourceDataPk"    = sl.suju_id
				 JOIN shipment_head shh ON shh.id = sh."ShipmentHead_id"
			   )
			   
			   SELECT
				 ev.*,
				 sc."Value" AS state_name
			   FROM ev
			   LEFT JOIN sys_code sc
				 ON sc."CodeType" = ev.code_type
				AND sc."Code"     = ev.state
			   ORDER BY ev.product_code ASC, ev.standard, ev.event_time ASC;
			   
		""";

        return this.sqlRunner.getRows(detailSql, paramMap);
	}

	// 발주 이력
	public List<Map<String, Object>> getBaljuHistory(int id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String detailSql = """ 
			WITH params AS (
				 SELECT :id::int AS head_id
			   ),
			   balju_lines AS (
				 SELECT
				   b.id                   AS balju_id,
				   b."BaljuHead_id"       AS head_id,
				   b."_created"           AS balju_created,
				   b."_creater_id"        AS balju_creater,
				   b."State"              AS balju_state,
				   b."SujuQty"            AS ordered_qty,     -- 발주수량
				   b."Material_id",
				   m."Code"               AS product_code,
				   m."Name"               AS product_name,
				   m."Standard1"          AS standard
				 FROM balju b
				 JOIN params p ON b."BaljuHead_id" = p.head_id
				 LEFT JOIN material m ON m.id = b."Material_id" AND m.spjangcd = b.spjangcd
			   ),
			   ev AS (
				 /* 1) 발주 이벤트(라인 1건) */
				 SELECT
				   'balju'              AS event_type,
				   bl.balju_id          AS event_pk,
				   bl.balju_created     AS event_time,
				   bl.balju_creater     AS actor_id,
				   bl.balju_state       AS state,             -- draft/partial/received...
				   NULL::text           AS note,
				   'balju_state'::text  AS code_type,
				   bl.balju_id,
				   bl.product_code,
				   bl.product_name,
				   bl.standard,
				   bl.ordered_qty,
				   NULL::numeric        AS qty,               -- 당 이벤트 수량
				   NULL::numeric        AS cum_qty            -- 누적 입고수량(발주 이벤트에는 없음)
				 FROM balju_lines bl
			   
				 UNION ALL
			   
				 /* 2) 입고 이벤트(여러 건, 누적 기준으로 상태 계산) */
				 SELECT
				   'input'              AS event_type,
				   mi.id                AS event_pk,
				   mi."_created"        AS event_time,
				   mi."_creater_id"     AS actor_id,
				   CASE
					 WHEN COALESCE(SUM(mi."InputQty") OVER (
							PARTITION BY mi."SourceDataPk"
							ORDER BY mi."_created", mi.id
							ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
						  ), 0) >= COALESCE(bl.ordered_qty, 0) THEN 'received'
					 WHEN COALESCE(SUM(mi."InputQty") OVER (
							PARTITION BY mi."SourceDataPk"
							ORDER BY mi."_created", mi.id
							ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
						  ), 0) > 0 THEN 'partial'
					 ELSE 'draft'
				   END                  AS state,
				   NULL::text           AS note,
				   'balju_state'::text  AS code_type,
				   bl.balju_id,
				   bl.product_code,
				   bl.product_name,
				   bl.standard,
				   bl.ordered_qty,
				   mi."InputQty"::numeric AS qty,             -- 이번 이벤트 입고 수량
				   SUM(mi."InputQty") OVER (
					 PARTITION BY mi."SourceDataPk"
					 ORDER BY mi."_created", mi.id
					 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				   )::numeric AS cum_qty                       -- 누적 입고 수량
				 FROM balju_lines bl
				 JOIN mat_inout mi
				   ON mi."SourceTableName" = 'balju'
				  AND mi."SourceDataPk"    = bl.balju_id
				  AND COALESCE(mi."_status", 'a') = 'a'
			   )
			   
			   SELECT
				 ev.*,
				 CASE
				   WHEN ev.event_type = 'balju' AND ev.state = 'draft' THEN '발주'  -- ← 요구사항: draft이면 '발주'
				   ELSE sc."Value"
				 END AS state_name
			   FROM ev
			   LEFT JOIN sys_code sc
				 ON sc."CodeType" = ev.code_type
				AND sc."Code"     = ev.state
			   ORDER BY
				 ev.product_code ASC,
				 ev.standard ASC NULLS LAST,
				 ev.event_time ASC,
				 ev.event_type ASC;
			   
		""";

        return this.sqlRunner.getRows(detailSql, paramMap);
	}

	//매출 이력
	public List<Map<String, Object>> getSalesHistory(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		String HistorySql= """
				 SELECT
				  'sales' AS event_type,
				  d.itemnm  AS product_name,
				  d.spec    AS standard,
				  COALESCE(sc."Value", m.taxtype) AS state_name,
				  (CASE
				     WHEN pg_typeof(m._created)::text = 'timestamptz'
				       THEN m._created
				     ELSE m._created AT TIME ZONE 'Asia/Seoul'
				   END) AS event_time
				FROM tb_salesdetail d
				JOIN tb_salesment  m ON m.misnum = d.misnum
				LEFT JOIN sys_code  sc
				       ON sc."CodeType" = 'tax_type'      -- 예: 과세/영세/면세 라벨 테이블이 있을 때
				      AND sc."Code"     = m.taxtype
				WHERE m.misnum = :id
				ORDER BY d.misseq::int;
				""";
		return this.sqlRunner.getRows(HistorySql, paramMap);
	}

	//매입 이력
	public List<Map<String, Object>> getInvoHistory(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		String HistorySql= """
				SELECT
				  'purchase' AS event_type, 
				  d.itemnm  AS product_name,
				  d.spec    AS standard,
				  COALESCE(sc."Value", m.misgubun, '매입') AS state_name,			
				  to_timestamp(m.misdate || '000000', 'YYYYMMDDHH24MISS')
				    AT TIME ZONE 'Asia/Seoul' AS event_time
				FROM tb_invoicedetail d
				JOIN tb_invoicement  m ON m.misnum = d.misnum
				LEFT JOIN sys_code sc
				       ON sc."CodeType" = 'purchase_type'
				      AND sc."Code"     = m.misgubun
				WHERE m.misnum = :id
				ORDER BY d.misseq::int;
				""";
		return this.sqlRunner.getRows(HistorySql, paramMap);
	}
	
	
	//입금 이력
	public List<Map<String, Object>> getReceiveHistory(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		String HistorySql= """
				SELECT
							'receive' AS event_type,
							NULL::text AS product_name,         
							NULL::text AS standard,
							COALESCE(s."Value", t.tradenm, '입금') AS state_name,
							to_timestamp(COALESCE(NULLIF(b.trdt, ''), b.trdate || '000000'), 'YYYYMMDDHH24MISS')
								AT TIME ZONE 'Asia/Seoul' AS event_time,
							COALESCE(b.accin, 0)  AS supply_amount,        -- 입금금액
						
							COALESCE(b.accin, 0) + COALESCE(b.feeamt, 0) AS total_amount,
							b.remark1 AS description
						FROM tb_banktransit b
						LEFT JOIN tb_trade t
									 ON t.trid = b.trid
						LEFT JOIN sys_code s
									 ON s."Code" = b.iotype
									AND s."CodeType" = 'deposit_type'
						WHERE b.ioflag = '0'   -- 입금
							AND b.ioid = :id
						ORDER BY event_time;
				""";
		return this.sqlRunner.getRows(HistorySql, paramMap);
	}

	//입금 이력
	public List<Map<String, Object>> getPaymentHistory(int id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		String HistorySql= """
				SELECT
				  'payment' AS event_type,
				  NULL::text AS product_name,                    -- 은행거래는 품목 없음
				  NULL::text AS standard,
				  COALESCE(s."Value", t.tradenm, '출금') AS state_name,
				  to_timestamp(COALESCE(NULLIF(b.trdt, ''), b.trdate || '000000'), 'YYYYMMDDHH24MISS')
				AT TIME ZONE 'Asia/Seoul' AS event_time,
				  COALESCE(b.accout, 0) AS supply_amount,        -- 출금금액
				  COALESCE(b.accout, 0) + COALESCE(b.feeamt, 0) AS total_amount,
				  b.remark1 AS description
				FROM tb_banktransit b
				LEFT JOIN tb_trade t
				       ON t.trid = b.trid
				LEFT JOIN sys_code s
				       ON s."Code" = b.iotype
				  AND s."CodeType" = 'deposit_type'   -- 출금 전용 코드타입이 따로 있으면 여기 변경
				WHERE b.ioflag = '1'   -- 출금
				  AND b.ioid = :id
				ORDER BY event_time;
				""";
		return this.sqlRunner.getRows(HistorySql, paramMap);
	}


	// 거래처 디테일
	public Map<String, Object> getCompany(int comp_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("comp_id", comp_id);

		String sql = """ 
			WITH shipment_status AS (
				 SELECT "SourceDataPk", SUM("Qty") AS shipped_qty
				 FROM shipment
				 WHERE "SourceTableName" = 'rela_data'
				 GROUP BY "SourceDataPk"
			 ),
			 suju_with_state AS (
				 SELECT
					 s.id,
					 s."SujuHead_id" as head_id,
					 s."Material_id",
					 m."Code" AS "product_code",
					 s."JumunDate",
					 m."Name" AS "product_name",
					 u."Name" AS "unit",
					 s."SujuQty" AS "quantity",
					 s."UnitPrice" AS "unit_price",
					 s."Vat" AS "vat_amount",
					 s."Price" AS "supply_amount",
					 s."TotalAmount" AS "total_amount",
					 s."Description" AS "description",
					 s."State" AS "original_state",
					 COALESCE(sh.shipped_qty, -1) AS "shipped_qty",
					  s."Standard" as  standard,
					 CASE
						 WHEN sh.shipped_qty = -1 THEN s."State"
						 WHEN sh.shipped_qty = 0 THEN 'force_completion'
						 WHEN sh.shipped_qty >= s."SujuQty" THEN 'shipped'
						 WHEN sh.shipped_qty < s."SujuQty" THEN 'partial'
						 ELSE s."State"
					 END AS final_state
				 FROM suju s
				 INNER JOIN material m ON m.id = s."Material_id"
				 INNER JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
				 LEFT JOIN unit u ON m."Unit_id" = u.id
				 LEFT JOIN TB_DA003 p ON p."projno" = s.project_id
				 LEFT JOIN shipment_status sh ON sh."SourceDataPk" = s.id
				 WHERE s."SujuHead_id" = :id
			 )

			 SELECT
				 s.id,
				 s.head_id,
				 s."Material_id",
				 s."product_code",
				 s."JumunDate",
				 s."product_name",
				 s."quantity",
				 s."unit_price",
				 s."vat_amount",
				 s."supply_amount",
				 s."total_amount",
				 s.final_state AS "state",
				 COALESCE(sc_ship."Value", sc_suju."Value") AS "state_name",
				 s."description",
				 s.standard
			 FROM suju_with_state s
			 LEFT JOIN sys_code sc_ship
				 ON sc_ship."Code" = s.final_state AND sc_ship."CodeType" = 'shipment_state'
			 LEFT JOIN sys_code sc_suju
				 ON sc_suju."Code" = s.final_state AND sc_suju."CodeType" = 'suju_state'
			 ORDER BY s.id
				 
		""";

        return this.sqlRunner.getRow(sql, paramMap);
	}


	public List<Map<String, Object>> todayWeekProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				with aa as (
						select * from job_res jr
						-- 금일
						where jr."ProductionDate" = current_date
						and jr."spjangcd" = :spjangcd
						), a1 as (
							-- 지시량
						     select sum(aa."OrderQty") as total_qty from aa where aa."State" !='canceled' 
						), a2 as(
							-- 완료량
						   select sum(aa."GoodQty") as good_qty from aa where aa."State" ='finished'
						), a3 as (
							-- 작업중
							select sum(aa."GoodQty") as working_qty from aa where aa."State" ='working'
						), bb as (
						select * from job_res jr
						-- 전일
						where jr."ProductionDate" = current_date - 1
						and jr."spjangcd" = :spjangcd
						), b1 as (
						     select sum(bb."OrderQty") as total_qty from bb where bb."State" !='canceled' 
						), b2 as(
						   select sum(bb."GoodQty") as good_qty from bb where bb."State" ='finished'
						), b3 as (
							select sum(bb."GoodQty") as working_qty from bb where bb."State" ='working'
						), cc as (
						select * from job_res jr
						-- 금주
						where jr."ProductionDate" between  date_trunc('week', current_date)::date and date_trunc('week', current_date)::date + 6
						and jr."spjangcd" = :spjangcd
						), c1 as (
						     select sum(cc."OrderQty") as total_qty from cc where cc."State" !='canceled' 
						), c2 as(
						   select sum(cc."GoodQty") as good_qty from cc where cc."State" ='finished'
						), c3 as (
							select sum(cc."GoodQty") as working_qty from cc where cc."State" ='working'
						), dd as (
						select * from job_res jr
						-- 전주
						where jr."ProductionDate" between date_trunc('week', current_date - 7)::date and date_trunc('week', current_date - 7)::date + 6
						and jr."spjangcd" = :spjangcd
						), d1 as (
						     select sum(dd."OrderQty") as total_qty from dd where dd."State" !='canceled' 
						), d2 as(
						   select sum(dd."GoodQty") as good_qty from dd where dd."State" ='finished'
						), d3 as (
							select sum(dd."GoodQty") as working_qty from dd where dd."State" ='working'
						)
						select '금일' as type
							, coalesce(a1.total_qty,0) as ord
							, coalesce(a2.good_qty,0) as com
							, coalesce(a3.working_qty,0) as wor
							, (case when a2.good_qty > 0 and a1.total_qty > 0 then trunc((a2.good_qty/a1.total_qty)*100) else 0 end) as wor_per 
						from a1 
						left join a2 on 1=1
						left join a3 on 1=1
						union all 
						select '전일'
						, coalesce(b1.total_qty,0)
						, coalesce(b2.good_qty,0)
						, coalesce(b3.working_qty,0)
						, (case when b2.good_qty > 0 and b1.total_qty > 0 then trunc((b2.good_qty/b1.total_qty)*100) else 0 end)
						from b1
						left join b2 on 1=1
						left join b3 on 1=1
						union all 
						select '금주'
						, coalesce(c1.total_qty,0)
						, coalesce(c2.good_qty,0)
						, coalesce(c3.working_qty,0)
						, (case when c2.good_qty > 0 and c1.total_qty > 0 then trunc((c2.good_qty/c1.total_qty)*100) else 0 end)
						from c1
						left join c2 on 1=1
						left join c3 on 1=1
						union all 
						select '전주'
						, coalesce(d1.total_qty,0)
						, coalesce(d2.good_qty,0)
						, coalesce(d3.working_qty,0)
						, (case when d2.good_qty > 0 and d1.total_qty > 0 then trunc((d2.good_qty/d1.total_qty)*100) else 0 end)
						from d1
						left join d2 on 1=1
						left join d3 on 1=1
					""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> todayProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
					select m."Name" as prod, mg."Name" as prod_grp
					, sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end) as ord
					, sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end) as com
					, sum(case when jr."State" = 'working' then jr."GoodQty" else 0 end) as wor
					, (case when sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end) > 0 
						and sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end) > 0 then 
						trunc((sum(case when jr."State" = 'finished' then jr."GoodQty" else 0 end)/sum(case when jr."State" != 'canceled' then jr."OrderQty" else 0 end)*100)) else 0 end) as wor_per 
					from job_res jr 
					inner join material m on jr."Material_id"  = m.id
					inner join mat_grp mg on m."MaterialGroup_id"  = mg.id
					where jr."ProductionDate"  = current_date
					and jr."spjangcd" = :spjangcd 
					group by m."Name", mg."Name" 
					order by ord desc
					limit 5
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> yearDefProd(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
			select mg."Name" as prod_grp , m."Name" as prod , coalesce(m."UnitPrice",0) as unitp
			,coalesce(sum(jrd."DefectQty") * m."UnitPrice",0) as dep
			,coalesce(sum(jrd."DefectQty"),0) as deq
			from job_res_defect jrd 
			left join job_res jr on jrd."JobResponse_id" = jr.id
			left join material m on jr."Material_id"  = m.id
			left join mat_grp mg on m."MaterialGroup_id"  = mg.id
			where to_char(jr."ProductionDate",'YYYY') = to_char(current_date,'YYYY')
			and jr."spjangcd" = :spjangcd
			group by mg."Name" , m."Name" , m."UnitPrice"
			having coalesce(sum(jrd."DefectQty"),0) > 0
			order by coalesce(sum(jr."DefectQty") * m."UnitPrice",0) desc
			limit 5
			""";
	
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public List<Map<String, Object>> matStock(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				select m."Name" as prod ,mg."Name" as prod_grp
				,coalesce(m."UnitPrice",0) as unitp
				,coalesce(sum(m."CurrentStock") * m."UnitPrice",0) as stp
				,coalesce(sum(m."CurrentStock"),0) as stq
				from material m 
				inner join mat_grp mg  on mg.id = m."MaterialGroup_id" 
				where m."CurrentStock" > 0
				and m."spjangcd" = :spjangcd
				and mg."MaterialType" = 'product'
				group by m."Name" , mg."Name" , m."UnitPrice" 
				order by coalesce(sum(m."CurrentStock") * m."UnitPrice",0) desc
				limit 5
				""";
		
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public List<Map<String, Object>> customOrder(String spjangcd) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
				select mg."Name" as prod_grp, fn_code_name('mat_type', mg."MaterialType" ) as mat_type_name, m."Name" as prod,m."Code" as prod_code ,coalesce(sum(s."SujuQty") * m."UnitPrice" ,0) as sujup
				from suju s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
				where to_char(s."JumunDate", 'YYYY') = to_char(current_date, 'YYYY') 
				and m."spjangcd" = :spjangcd
				group by m."Name" , mg."Name" ,m."UnitPrice", m."Code" , mg."MaterialType"
				having coalesce(sum(s."SujuQty") * m."UnitPrice" ,0) > 0
				order by sujup desc
				limit 10
				""";
		
	List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
	
	return items;
	}

	public Map<String, Object> customServiceStat(String dateType) {
		
		
		String sql = """
				with cte as (
					select * from cust_complain cc
				""";
		
		if (dateType.equals("Year")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY') = to_char(current_date,'YYYY') ";
		} else if (dateType.equals("Mon")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY-MM') = to_char(current_date,'YYYY-MM') ";
		}
		
		sql += """
				), total as (
				select 	 coalesce(sum(cte."Qty"),0) as "totalCnt"
						, coalesce(sum(case when cte."CheckState" = '조치중' then cte."Qty" else 0 end),0) as working
						, coalesce(sum(case when cte."CheckState" = '조치완료' then cte."Qty" else 0 end),0) as finish
				from cte
				) 
				select  * from total
				""";
		
		Map<String, Object> total = this.sqlRunner.getRow(sql, null);
		
		sql = """
				with cte as (
					select * from cust_complain cc
				""";
		
		if (dateType.equals("Year")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY') = to_char(current_date,'YYYY') ";
		} else if (dateType.equals("Mon")) {
			sql += " where to_char(cc.\"CheckDate\",'YYYY-MM') = to_char(current_date,'YYYY-MM') ";
		}
		
		sql += """
				), total as (
				select 	 cte."Type",sum(cte."Qty") as "totalCnt"
				from cte
				group by cte."Type"
				) 
				select  * from total
				""";
		
		List<Map<String, Object>> typeList = this.sqlRunner.getRows(sql, null);
		
		Map<String,Object> items = new HashMap<>();
		
		items.put("total", total);
		items.put("typeList", typeList);
		
		
		return items;
	}

	public List<Map<String, Object>> customServiceStatResult() {
		
		String sql = """
				select 
					cc."Type" as name
				""";
		
		for (int i = 1; i < 13; i++) {
				sql += 	" , sum(case when cast(to_char(cc.\"CheckDate\", 'MM') as integer) = "+ i + "then cc.\"Qty\" else 0 end) as p"+ i + " ";
		}
		
		sql += """
				from cust_complain cc
				where to_char(cc."CheckDate", 'YYYY') = to_char(current_date, 'YYYY') 
				group by cc."Type" 
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, null);
		
		return items;
	}

	public Map<String, Object> haccpReadResult(String year_month, String data_year,String data_month,Authentication auth) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("year_month", year_month);
		paramMap.addValue("data_year", data_year);
		paramMap.addValue("data_month", data_month);
		
		User user = (User)auth.getPrincipal();
		
		Integer userId = user.getId();
		paramMap.addValue("userId", userId);
		
		
		
		String sql = "";
		
		
		
		sql = """
				with u as (
				select u."User_id", g.id as "UserGroup_id",u."Name"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, ar as (
				select rank() over(partition by "TaskMasterCode" order by "ApprDate" desc) as rn 
				, "ApprDate", "TaskMasterCode", "StateName", "LineName","OriginGui","State"
				from v_appr_result
			)
			, re as(
			select distinct tm.id, sc."Value" as code_group_name,"State",tm."Code" as code, tm."TaskName" as task_name, to_char(ar."ApprDate", 'yyyy-MM-dd') as last_appr_date, ar."StateName" as state_name
				, "OriginGui" as menu_link,ta."User_id",tm."WriterGroup_id"--,tm."User_id"
			from task_master tm
			inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
			left join task_approver ta on tm.id = ta."TaskMaster_id" 
			left join u on tm."WriterGroup_id" = u."UserGroup_id"
			left join ar on tm."Code" = ar."TaskMasterCode" and ar.rn = 1
			where 1=1
			and ta."User_id" =:userId or tm."WriterGroup_id" in (
									select distinct "UserGroup_id"
									from task_approver ta
									inner join user_profile up on ta."User_id" = up."User_id" 
									where ta."User_id" = :userId)
			)		
			select coalesce(sum(case when "State"='process' then 1 else 0 end), 0) as process_count
				, coalesce(sum(case when "State"='approval' then 1 else 0 end), 0) as approval_count
				, coalesce(sum(case when "State"='reject' then 1 else 0 end), 0) as reject_count
			from re				
			""";
		Map<String, Object> appr_list = this.sqlRunner.getRow(sql, paramMap);
		
		
		sql="""
				with u as (
				select u."User_id", g.id as "UserGroup_id",u."Name"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, ar as (
				select rank() over(partition by "TaskMasterCode" order by "ApprDate" desc) as rn 
				, "ApprDate", "TaskMasterCode", "StateName", "LineName","OriginGui","State"
				from v_appr_result
			)
			select distinct tm.id, sc."Value" as code_group_name,"State",tm."Code" as code, tm."TaskName" as task_name, to_char(ar."ApprDate", 'yyyy-MM-dd') as last_appr_date, ar."StateName" as state_name
				, public.fn_prop_data_char('task_master', tm.id, 'menu_link') as menu_link,ta."User_id",tm."WriterGroup_id"--,tm."User_id"
			from task_master tm
			inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
			left join task_approver ta on tm.id = ta."TaskMaster_id" 
			left join u on tm."WriterGroup_id" = u."UserGroup_id"
			left join ar on tm."Code" = ar."TaskMasterCode" and ar.rn = 1
			where 1=1
			and ta."User_id" =:userId or tm."WriterGroup_id" in (
									select distinct "UserGroup_id"
									from task_approver ta
									inner join user_profile up on ta."User_id" = up."User_id" 
									where ta."User_id" = :userId)
			order by sc."Value", tm."TaskName"
				""";
		
		List<Map<String, Object>> total_busi_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		sql="""
				with u as (
				select u."User_id", g.id as "UserGroup_id", g."Code" as "UserGroup_code"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, dd as (
				select 'Y' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				union all
				select 'H' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by case when data_month in (1,2,3,4,5,6) then 1 else 2 end
				union all
				select 'Q' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by case when data_month in (1,2,3) then 1 when data_month in (4,5,6) then 2 when data_month in (7,8,9) then 3 else 4 end
				union all
				select 'M' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
				group by data_month
				union all
				select 'W' as data_div, min(data_date) as from_date, max(data_date) as to_date 
				from dummy_date 
				where yw_year = cast(to_char(now(), 'yyyy') as Integer)
				group by yw_week
				union all
				select 'D' as data_div, data_date as from_date, data_date as to_date 
				from dummy_date 
				where data_year = cast(to_char(now(), 'yyyy') as Integer)
			)
			, task_t as (
				select sc."Value" as code_group_name, tm."TaskName" as task_name, tm."CycleBase" as cycle_base, pd."Char1" as cycle_check, dd.from_date, dd.to_date
					, count(ar."ApprDate") as write_count
					, case when count(ar."ApprDate")>0 then 'Y' else 'N' end as write_yn
				from task_master tm
				inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
				inner join u on tm."WriterGroup_id" = u."UserGroup_id"
				inner join prop_data pd on tm.id = pd."DataPk" and "TableName" = 'task_master' 
					and (
						(tm."CycleBase" in ('W','M','Y') and pd."Code" in ('cycle_date1'))
						or (tm."CycleBase"='Q' and pd."Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4'))
						or (tm."CycleBase"='H' and pd."Code" in ('cycle_date1', 'cycle_date2'))
					)
				inner join dd on tm."CycleBase" = dd.data_div and now() between cast(dd.from_date as date ) and dd.to_date+(interval '0.99999 DAY')
				left join v_appr_result ar on tm."Code" = ar."TaskMasterCode" and ar."ApprDate" between cast(dd.from_date as date) and cast(dd.to_date as date)+(interval '0.99999 DAY')
				where u."User_id" = :userId
				and public.fn_task_cycle_check(upper(tm."CycleBase"), pd."Char1", now()::date) = 'Y'
				group by sc."Value", tm."TaskName", tm."CycleBase", pd."Char1", dd.from_date, dd.to_date
			)
			select *
			from task_t
			order by code_group_name, task_name
				""";
		
		
		List<Map<String, Object>> today_busi_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		sql = """
				with u as (
				select u."User_id", g.id as "UserGroup_id", g."Code" as "UserGroup_code"
				from user_profile u
				inner join rela_data r on u."User_id" = r."DataPk1" and r."RelationName" = 'auth_user-user_group'
				inner join user_group g on r."DataPk2" = g.id and r."RelationName" = 'auth_user-user_group'
			)
			, task_t as (
				select sc."Value" as code_group_name, tm."TaskName" as task_name, tm."CycleBase" as cycle_base, pd."Char1" as cycle_check--, dd.from_date, dd.to_date
					, dd.data_date
				from task_master tm
				inner join sys_code sc on tm."GroupCode" = sc."Code" and sc."CodeType" = 'task_group_code'
				inner join u on tm."WriterGroup_id" = u."UserGroup_id"
				inner join prop_data pd on tm.id = pd."DataPk" and "TableName" = 'task_master' 
					and (
						(tm."CycleBase" in ('W','M','Y') and pd."Code" in ('cycle_date1'))
						or (tm."CycleBase"='Q' and pd."Code" in ('cycle_date1', 'cycle_date2', 'cycle_date3', 'cycle_date4'))
						or (tm."CycleBase"='H' and pd."Code" in ('cycle_date1', 'cycle_date2'))
					)
				inner join dummy_date dd on dd.data_year=(:data_year::Integer) and dd.data_month=(:data_month::Integer) and public.fn_task_cycle_check(upper(tm."CycleBase"), pd."Char1", dd.data_date) = 'Y'
				where u."User_id" = :userId
			)
			, calib_t as (
				select t."Name" as "Name", t."CycleBase", t."CycleNumber", t."SourceTableName", r."CalibDate"
					,to_char(coalesce(
						case when t."CycleBase" = 'Y' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' year')::interval
							when t."CycleBase" = 'M' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' month')::interval 
							else r."CalibDate"
							end
					,now()),'yyyy-MM-dd') "NextCalibDate"
				from calib_inst t
				inner join u on (
					select ug."Code"
					from user_profile up 
					inner join user_group ug on up."UserGroup_id" = ug.id
					where up."User_id" = :userId) = u."UserGroup_code"
				left join (
					select "CalibInstrument_id", max("CalibDate") as "CalibDate"
					from calib_result
					group by "CalibInstrument_id"
				) r on t.id = r."CalibInstrument_id"
				where u."User_id" = :userId
				and to_char(coalesce(
						case when t."CycleBase" = 'Y' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' year')::interval
							when t."CycleBase" = 'M' and r."CalibDate" is not null then r."CalibDate" + ("CycleNumber"::text||' month')::interval 
							else r."CalibDate"
							end
					,now()),'yyyy-MM')  = :data_year||:data_month
			)
			select *
			from task_t
			union all
			select '검교정관리', "Name"||' 검교정', "CycleBase", "CycleNumber"::text, "NextCalibDate"::date
			from calib_t
			order by code_group_name, task_name
				""";
		
		
		List<Map<String, Object>> calendar_list = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		
		
		Map<String, Object> items = new HashMap<>();
		
		items.put("appr_list", appr_list);
		items.put("total_busi_list", total_busi_list);
		items.put("today_busi_list", today_busi_list);
		items.put("calendar_list", calendar_list);
		
		
		
		return items;
	}

	public Map<String, Object> getCppList(String strDate,Authentication auth) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("strDate",strDate);
		
		String sql = "";
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		,hd."DataDate"
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-2B_02'
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where hp."Code"='CCP-2B_02'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name 
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name 
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		List<Map<String, Object>> getCcpRoastList = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, hd."DataDate"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-2B_01'
		--where  ht."HaccpDiary_id" = :hd_id
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where  hp."Code" = 'CCP-2B_01'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		
		List<Map<String, Object>> getccpMbathList = this.sqlRunner.getRows(sql, paramMap);
		
		
		
		
		
		sql = """
				with A as(
		select ht."HaccpDiary_id"
		, hd."HaccpProcess_id"
		, ht.id as ht_id
		, ht."DataType"
		, substring( ht."StartTime"::text,1, 5) as "StartTime"
		, hd."DataDate"
		, ht."EndTime"
		, ht."Material_id"
		, m."Name" as "MaterialName"
		, m."Code" as mat_code
		, ht."Equipment_id"
		, e."Name" as equ_name
		, ht."Judge" , ht."TesterName" , ht."Description"
		, null::integer as hir_id, null::integer as item_id,null as item_name, null as unit_name
		, null::float as "NumResult", null::float as "LowSpec", null::float as "UpperSpec", null as "SpecText"
		, 0 as _order
		from  haccp_test ht 
		inner join haccp_diary hd  on ht."HaccpDiary_id" = hd.id
		inner join haccp_proc hp on hp.id = hd."HaccpProcess_id"
		left join material m on m.id = ht."Material_id"
		left join equ e on e.id= ht."Equipment_id"
		where hd."DataDate" between (:strDate::date) - interval '2 week' and (:strDate::date)
		and hp."Code" = 'CCP-3P'
		--where  ht."HaccpDiary_id" = :hd_id
		), B as (
		select null::integer as "HaccpDiary_id"
		, null::integer as "HaccpProcess_id"
		, tt.ht_id
		, tt."DataType"
		, tt."StartTime"
		 ,tt."EndTime"
		, null::integer as "Material_id"
		, null as"MaterialName"
		, null::integer as "Equipment_id"
		, null::text as equ_name
		, null as "Judge", null as "TesterName", null as "Description"
		, hir.id as hir_id, hi.id as item_id, hi."Name" as item_name, u."Name" as unit_name
		, case when hir."NumResult" is null then hir."CharResult"
		when hir."NumResult" is not null then hir."NumResult"::text end as "NumResult"
		, hil."LowSpec" , hil."UpperSpec", hil."SpecText" 
			, hpi._order
		   from A tt
		  --  inner join haccp_test ht on tt."HaccpDiary_id" = ht."HaccpDiary_id" 
		--and ht.id = tt.ht_id
		inner join haccp_proc hp on hp.id = tt."HaccpProcess_id"
		inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id
		inner join haccp_item hi on hi.id = hpi."HaccpItem_id"
		left join unit u on u.id = hi."Unit_id"
		left join haccp_item_result hir on hir."HaccpTest_id" = tt.ht_id
		and hir."HaccpItem_id"= hpi."HaccpItem_id"
		left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id  
		and tt."Material_id" = hil."Material_id" 
		and hil."HaccpItem_id" = hpi."HaccpItem_id" 
		where  hp."Code" = 'CCP-3P'
		)
		select 1 as t_lvl, "HaccpDiary_id", "HaccpProcess_id","DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult"::text, "LowSpec", "UpperSpec", "SpecText", _order
		from A
		union all
		select 2 as t_lvl, "HaccpDiary_id", "HaccpProcess_id",to_date('','YYYY-MM-DD') as "DataDate" , ht_id, "DataType", "StartTime", "EndTime", "Material_id",'' as mat_code, "MaterialName", "Equipment_id", equ_name
		, "Judge", "TesterName", "Description", hir_id, item_id, item_name, unit_name
		, "NumResult", "LowSpec", "UpperSpec", "SpecText", _order
		from B 
		order by "StartTime", ht_id, t_lvl, _order;
				""";
		
		
		List<Map<String, Object>> getccpFmatterList = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> items =new HashMap<>();
		items.put("getCcpRoastList", getCcpRoastList);
		items.put("getccpMbathList", getccpMbathList);
		items.put("getccpFmatterList", getccpFmatterList);
		
		
		return items;
	}

	public Map<String, Object> getDetailHacpPro() {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		String sql = "";
		
		sql = """
				select hpi.id as hpi_id
	            , hpi."HaccpProcess_id" as hp_id
	            , hpi."HaccpItem_id" as item_id
	            , hp."Name" as haccp_process_name
	            , hi."Name" as item_name
	            , hi."ResultType"
	            , u."Name" as unit_name
	            , hpi."_order"
	            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
	            FROM haccp_proc_item hpi 
	            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
	            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
	            left join unit u on u.id = hi."Unit_id"
	            where hp."Code" = 'CCP-2B_02'
	            order by hpi."_order"   
				""";
		
		 List<Map<String, Object>> getCcpRoastHead = this.sqlRunner.getRows(sql, paramMap);
		 
		 sql = """
					select hpi.id as hpi_id
		            , hpi."HaccpProcess_id" as hp_id
		            , hpi."HaccpItem_id" as item_id
		            , hp."Name" as haccp_process_name
		            , hi."Name" as item_name
		            , hi."ResultType"
		            , u."Name" as unit_name
		            , hpi."_order"
		            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
		            FROM haccp_proc_item hpi 
		            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
		            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
		            left join unit u on u.id = hi."Unit_id"
		            where hp."Code" = 'CCP-2B_01'
		            order by hpi."_order"   
					""";
			
			 List<Map<String, Object>> getccpMbathHead = this.sqlRunner.getRows(sql, paramMap);
			 
			 
		 sql = """
					select hpi.id as hpi_id
		            , hpi."HaccpProcess_id" as hp_id
		            , hpi."HaccpItem_id" as item_id
		            , hp."Name" as haccp_process_name
		            , hi."Name" as item_name
		            , hi."ResultType"
		            , u."Name" as unit_name
		            , hpi."_order"
		            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
		            FROM haccp_proc_item hpi 
		            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
		            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
		            left join unit u on u.id = hi."Unit_id"
		            where hp."Code" = 'CCP-3P'
		            order by hpi."_order"   
					""";
			
			 List<Map<String, Object>> getFmatterHead = this.sqlRunner.getRows(sql, paramMap);
			 
			 Map<String, Object> items =new HashMap<>();
			 items.put("getCcpRoastHead", getCcpRoastHead);
			 items.put("getccpMbathHead", getccpMbathHead);
			 items.put("getFmatterHead", getFmatterHead);
	        
	     return items;
	}


	public List<Map<String, Object>> getJobStateRead(String dateFrom, String dateTo, String isIncludeComp, String spjangcd, String choMat, Integer cboFactory, String company) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("dateFrom", dateFrom);
		dicParam.addValue("dateTo", dateTo);
		dicParam.addValue("isIncludeComp", isIncludeComp);
		dicParam.addValue("spjangcd", spjangcd);
		dicParam.addValue("cboFactory", cboFactory);
		String pattern = (choMat == null || choMat.isBlank()) ? "%" : "%" + choMat + "%";
		dicParam.addValue("choMat", pattern);

		String sql = """
			WITH T AS (
				 SELECT
					 jr.id AS child_id,
					 jr."Parent_id" AS parent_id,
					 jr."Description" AS memo,
					 COALESCE(jr."Parent_id", jr.id) AS base_id,
					 CASE WHEN jr."State"='working' THEN 1 ELSE 0 END AS is_working,
					 CASE WHEN jr."State"='stopped' THEN 1 ELSE 0 END AS is_stopped
				 FROM job_res jr
				 WHERE jr."ProductionDate" BETWEEN CAST(:dateFrom AS date) AND CAST(:dateTo AS date)
					 AND jr.spjangcd = :spjangcd
			 ),
	
			 S AS (
				 SELECT
					 T.*,
					 ROW_NUMBER() OVER (
						 PARTITION BY T.base_id
						 ORDER BY
							 T.is_working DESC,
							 CASE WHEN T.parent_id IS NULL THEN 1 ELSE 0 END DESC,
							 T.child_id DESC
					 ) AS rn,
					 MAX(T.is_working) OVER (PARTITION BY T.base_id) AS any_working,
					 MAX(T.is_stopped) OVER (PARTITION BY T.base_id) AS any_stopped
				 FROM T
			 ),
	
			 base_suju AS (
				 SELECT DISTINCT
					 B."SourceDataPk" AS suju_id
				 FROM S
				 JOIN job_res B ON B.id = S.base_id
				 WHERE B."SourceTableName" = 'suju'
					 AND B."SourceDataPk" IS NOT NULL
			 ),
	
			 shipped_by_suju AS (
				 SELECT
					 shp."SourceDataPk" AS suju_id,
					 SUM(shp."Qty") AS shipped_qty
				 FROM shipment shp
				 JOIN base_suju bs ON bs.suju_id = shp."SourceDataPk"
				 GROUP BY shp."SourceDataPk"
			 ),
	
			 ship_state_by_suju AS (
				 SELECT
					 su.id AS suju_id,
					 CASE
						 WHEN sb.suju_id IS NULL THEN ''
						 WHEN COALESCE(sb.shipped_qty, 0) = 0 THEN 'ordered'
						 WHEN COALESCE(sb.shipped_qty, 0) >= su."SujuQty" THEN 'shipped'
						 WHEN COALESCE(sb.shipped_qty, 0) <  su."SujuQty" THEN 'partial'
						 ELSE ''
					 END AS shipment_state
				 FROM suju su
				 JOIN base_suju bs ON bs.suju_id = su.id
				 LEFT JOIN shipped_by_suju sb ON sb.suju_id = su.id
			 ),
	
			 F AS (
				 SELECT
					 S.child_id AS id,
					 C."WorkOrderNumber" AS order_num,
					 TO_CHAR(B."ProductionDate",'yyyy-mm-dd') AS prod_date,
					 TO_CHAR(su."DueDate",'yyyy-mm-dd') AS due_date,
	
					 CASE
						 WHEN S.any_working = 1 THEN 'working'
						 WHEN S.any_stopped = 1 THEN 'stopped'
						 ELSE B."State"
					 END AS state,
	
					 fn_code_name('job_state',
						 CASE
							 WHEN S.any_working = 1 THEN 'working'
							 WHEN S.any_stopped = 1 THEN 'stopped'
							 ELSE B."State"
						 END
					 ) AS job_state,
	
					 M."Name" AS mat_name,
					 M."Factory_id" AS "Factory_id",
					 U."Name" AS unit,
					 COALESCE(su."Standard", M."Standard1") AS standard,
					 su."CompanyName" AS company_name,
					 ROUND(B."OrderQty"::numeric, 2)  AS order_qty,
					 ROUND(B."GoodQty"::numeric, 2)   AS good_qty,
					 ROUND(B."DefectQty"::numeric, 2) AS defect_qty,
					 S.memo,
					 su.id AS suju_id,
	
					 ssbs.shipment_state AS "ShipmentState",
					 fn_code_name('shipment_state', ssbs.shipment_state) AS "ShipmentStateName"
	
				 FROM S
				 JOIN job_res C ON C.id = S.child_id
				 JOIN job_res B ON B.id = S.base_id
				 LEFT JOIN material M ON M.id = B."Material_id"
				 LEFT JOIN unit U ON U.id = M."Unit_id"
				 LEFT JOIN suju su ON su.id = B."SourceDataPk" AND B."SourceTableName" = 'suju'
				 LEFT JOIN ship_state_by_suju ssbs ON ssbs.suju_id = su.id
				 WHERE S.rn = 1
			 )
	
			 SELECT *
			 FROM F
			 WHERE 1=1
				 AND F.mat_name LIKE :choMat
			""";

		if ("false".equalsIgnoreCase(isIncludeComp)) {
			// ★ 파생 상태(state) 기준으로 완료 제외
			sql += " and F.state != 'finished' ";
		}

		if (cboFactory != null) {
			sql += " and F.\"Factory_id\" = :cboFactory ";
		}

		if (StringUtils.isEmpty(company) == false) {
			sql += " AND F.company_name LIKE :company ";
			dicParam.addValue("company", "%" + company + "%");
		}

		sql += " ORDER BY F.prod_date desc, F.order_num, F.id ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
}
