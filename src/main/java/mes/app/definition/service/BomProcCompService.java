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

  public List<Map<String, Object>> getBomProcCompList(String routing_name, String spjangcd) {

    MapSqlParameterSource param = new MapSqlParameterSource();
    param.addValue("routing_name", routing_name);
    param.addValue("spjangcd", spjangcd);

    String sql = """
        SELECT
             bpc.id                                 AS id,               -- bom_proc_comp_id
             bpc."BOM_id"                           AS bom_id,
             bpc."Routing_id"                       AS routing_pk,
             bpc."Product_id"                       AS prod_pk,
             bpc."Material_id"  as mat_id ,
             mat2."Name"  as mat_name,\s
             p.id 									 as process_id, \s
             p."Code"                               AS process_code,
             p."Name"                               AS process_name,
             mg."Name"                              AS prod_grp_name,  
             mat."Code"                            AS prod_code,
             mat."Name"                            AS prod_name,
             COALESCE(bpc."Amount", 0)              AS amount
           FROM public.bom_proc_comp bpc
           LEFT JOIN public.process       p   ON p.id   = bpc."Process_id"
           LEFT JOIN public.routing       r   ON r.id   = bpc."Routing_id"
           LEFT JOIN public.bom           b   ON b.id   = bpc."BOM_id"
           LEFT JOIN public.material     mat ON mat.id = bpc."Product_id"
           LEFT JOIN public.material     mat2 ON mat2.id = bpc."Material_id"
           LEFT JOIN public.mat_grp mg   ON mg.id  = mat."MaterialGroup_id"
           WHERE bpc.spjangcd = :spjangcd
           -- AND bpc."Product_id" = :productId
           -- AND bpc."Routing_id" = :routingId
           ORDER BY p."Name", mg."Name", mat."Name"
        """;
    return this.sqlRunner.getRows(sql, param);
  }
}
