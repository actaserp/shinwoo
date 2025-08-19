package mes.app.definition.service;

import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BomProcCompService {

  @Autowired
  SqlRunner sqlRunner;

  public List<Map<String, Object>> getRoutingList(String routing_name, String spjangcd) {

    MapSqlParameterSource param = new MapSqlParameterSource();
    param.addValue("routing_name", routing_name);
    param.addValue("spjangcd", spjangcd);

    String sql = """
        WITH r AS (
           SELECT
             rt.id,
             rt."Name" AS routing_name,
             COALESCE(rt."Description",'') AS routing_desc_raw,
             rt.spjangcd
           FROM public.routing rt
           WHERE COALESCE(:routing_name, '') = ''
                 OR rt."Name" ILIKE '%' || :routing_name || '%'
         ),
         pc AS (  -- 공정수 (routing_proc 기준)
           SELECT
             rp."Routing_id" AS routing_id,
             COUNT(*)::int AS proc_count
           FROM public.routing_proc rp
           GROUP BY rp."Routing_id"
         ),
         prodc AS (  -- 해당제품수 = DISTINCT Product_id (산출품 기준)
           SELECT
             bpc."Routing_id" AS routing_id,
             COUNT(DISTINCT bpc."Product_id")::int AS mat_count
           FROM public.bom_proc_comp bpc
           WHERE bpc."Routing_id" IS NOT NULL
           GROUP BY bpc."Routing_id"
         ),
         prodcodes AS ( -- (반)제품코드 리스트 (산출품 기준)
           SELECT
             bpc."Routing_id" AS routing_id,
             string_agg(DISTINCT p."Code", ', ' ORDER BY p."Code") AS product_codes,
             p."Name"
           FROM bom_proc_comp bpc
           JOIN material p ON p.id = bpc."Product_id"
           WHERE bpc."Routing_id" IS NOT NULL
           GROUP BY bpc."Routing_id",  p."Name"
         )
         SELECT
           r.id AS routing_id,                           -- onClick에서 사용할 PK
           r.routing_name,                               -- 라우팅명
           COALESCE(pc.proc_count, 0) AS proc_count,     -- 공정수
           COALESCE(prodc.mat_count, 0) AS mat_count,    -- 해당제품수(=DISTINCT Product_id)
           prodcodes.product_codes  AS routing_desc  -- (반)제품코드
         FROM r
         LEFT JOIN pc        ON pc.routing_id = r.id
         LEFT JOIN prodc     ON prodc.routing_id = r.id
         LEFT JOIN prodcodes ON prodcodes.routing_id = r.id
         where r.spjangcd = :spjangcd
         ORDER BY r.routing_name;
        """;

    return this.sqlRunner.getRows(sql, param);
  }
}
