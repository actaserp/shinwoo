package mes.app.schedule.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatRequSimulationService {

	@Autowired
	SqlRunner sqlRunner;
	
	//제품별 수주량 만큼 원부자재 소요량의 전체 레벨의 합계를 보여준다
	public Map<String, Object> getMatRequSimulationList(
			String prod_pks, String order_qtys, String base_date) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("prod_pks", prod_pks);
		paramMap.addValue("order_qtys", order_qtys);
		paramMap.addValue("base_date", base_date);

		String sql = """
           with recursive
				       A as (
				           -- prod_pks, order_qtys를 같이 받아서 (1,2,3 / 10,20,30) 처럼 매핑
				           select unnest(string_to_array(:prod_pks, ','))::int as prod_pk,
				                  unnest(string_to_array(:order_qtys, ','))::numeric as order_qty
				       ),
				       bom_tree as (
				           -- root: 제품 → bom_comp
				           select 1 as lvl,
				                  a.prod_pk,
				                  b."Material_id"        as parent_mat_id,
				                  bc."Material_id"       as mat_id,
				                  a.order_qty,
				                  bc."Amount"::numeric   as qty,
				                  coalesce(b."OutputAmount",1) as produced_qty,
				                  (bc."Amount" / coalesce(b."OutputAmount",1))::numeric as bom_ratio,
				                  (a.order_qty * (bc."Amount" / coalesce(b."OutputAmount",1)))::numeric as requ_qty
				           from A
				           join bom b on b."Material_id" = a.prod_pk
				           join bom_comp bc on bc."BOM_id" = b.id
				       
				           union all
				       
				           -- children: 부모 requ_qty 승계
				           select t.lvl+1,
				                  t.prod_pk,
				                  t.mat_id                  as parent_mat_id,
				                  bc."Material_id"          as mat_id,
				                  t.order_qty,
				                  bc."Amount"::numeric,
				                  coalesce(b."OutputAmount",1),
				                  (bc."Amount" / coalesce(b."OutputAmount",1))::numeric,
				                  (t.requ_qty * (bc."Amount" / coalesce(b."OutputAmount",1)))::numeric as requ_qty
				           from bom_tree t
				           join bom b on b."Material_id" = t.mat_id
				           join bom_comp bc on bc."BOM_id" = b.id
				       ),
				       R as (
				           select t.mat_id,
				                  mg."MaterialType" as mat_type,
				                  fn_code_name('mat_type', mg."MaterialType") as mat_type_name,
				                  mg."Name" as mat_group_name,
				                  m."Code" as mat_code,
				                  m."Name" as mat_name,
				                  u."Name" as unit_name,
				                  sum(t.requ_qty) as requ_qty,
				                  m."CurrentStock" as cur_stock,
				                  m."AvailableStock" as available_stock
				           from bom_tree t
				           join material m on m.id = t.mat_id
				           join mat_grp mg on mg.id = m."MaterialGroup_id"
				           left join unit u on u.id = m."Unit_id"
				           group by t.mat_id, mg."MaterialType", mg."Name",
				                    m."Code", m."Name", u."Name",
				                    m."CurrentStock", m."AvailableStock"
				       )
				       select *
				       from R
				       order by mat_type, mat_group_name, mat_code, mat_name;
				       
    """;

		List<Map<String,Object>> rows = this.sqlRunner.getRows(sql, paramMap);

		List<Map<String,Object>> rawList = new ArrayList<>();
		List<Map<String,Object>> banList = new ArrayList<>();

		for (Map<String,Object> row : rows) {
			String matType = (String) row.get("mat_type");
			if ("jajae".equals(matType) || "sub_mat".equals(matType)) {
				rawList.add(row);   // 원자재/부자재
			} else if ("semi".equals(matType) || "product".equals(matType)) {
				banList.add(row);   // 반제품
			}
		}

		Map<String,Object> items = new HashMap<>();
		items.put("raw_list", rawList);
		items.put("ban_list", banList);
		return items;

	}

}
