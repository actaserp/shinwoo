package mes.app.production.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ProdPrepareService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 작업지시내역 조회
	public List<Map<String, Object>> jobOrderSearch(String data_date, String shift_code, Integer workcenter_pk, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("data_date", Date.valueOf(data_date));  
		paramMap.addValue("shift_code", shift_code);  
		paramMap.addValue("workcenter_pk", workcenter_pk);
		paramMap.addValue("spjangcd", spjangcd);
		
        String sql = """
        		select jr.id
                , jr."WorkOrderNumber" as work_order_number
                , to_char(jr."ProductionDate", 'yyyy-mm-dd') as production_date
                , jr."ShiftCode" as shift_code, sh."Name" as shift_name
                , wc."Name" as workcenter_name
                , jr."WorkIndex" as work_index
                , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                , mg."Name" as mat_grp_name
                , m."Code" as mat_code, m."Name" as mat_name, u."Name" as unit_name
                , jr."OrderQty" as order_qty
                , e."Name" as equip_name
                , jr."State" as state, fn_code_name('job_state', jr."State") as state_name
                , jr."Description" as description
                , jr."MaterialProcessInputRequest_id" as proc_input_req_id
                , jr."State"
                , fn_code_name('job_state', jr."State") as state_name
                , COALESCE(s."Standard", m."Standard1") as standard
                from job_res jr 
                left join material m on m.id = jr."Material_id"
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
                left join work_center wc on wc.id = jr."WorkCenter_id"
                left join equ e on e.id = jr."Equipment_id"
                left join shift sh on sh."Code" = jr."ShiftCode"
                left join suju s on s.id = jr."SourceDataPk" and jr."SourceTableName" = 'suju'
                where jr."ProductionDate" = :data_date and jr."State" = 'ordered'
                and jr.spjangcd = :spjangcd
                and jr."Parent_id" is null
        		""";
  		
	    if (StringUtils.isEmpty(shift_code) == false) {
	    	sql +=" and jr.\"ShiftCode\" = :shift_code ";
	    }
	    
        if (workcenter_pk != null) {
        	sql += " and jr.\"WorkCenter_id\" = :workcenter_pk ";
        }
          

        sql += " order by jr.\"ProductionDate\", jr.\"WorkIndex\", jr.\"ShiftCode\", jr.id ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	// 작업지시내역 조회(그냥 트리 버전)
//	public List<Map<String, Object>> bomDetailList(String jr_pks, String data_date) {
//
//		MapSqlParameterSource paramMap = new MapSqlParameterSource();
//		paramMap.addValue("jr_pks", jr_pks);
//		paramMap.addValue("data_date", Date.valueOf(data_date));
//
//        String sql = """
//			with recursive
//				A as (
//					select unnest(string_to_array(:jr_pks, ','))::int as jr_pk
//				),
//				jr as (
//					select jr.id as jr_id,
//						   jr."Material_id" as prod_pk,
//						   jr."OrderQty" as order_qty
//					from A
//					join job_res jr on jr.id = A.jr_pk
//					where jr."MaterialProcessInputRequest_id" is null
//					  and jr."State" in ('ordered','working')
//				),
//				bom_tree as (
//					-- root: job_res → bom → bom_comp
//					select 1 as lvl,
//						   jr.jr_id,
//						   b."Material_id"        as parent_mat_id,
//						   bc."Material_id"       as mat_id,
//						   jr.prod_pk,
//						   jr.order_qty,
//						   bc."Amount"::numeric   as qty,
//						   coalesce(b."OutputAmount",1) as produced_qty,
//						   (bc."Amount" / coalesce(b."OutputAmount",1))::numeric as bom_ratio,
//						   (jr.order_qty * (bc."Amount" / coalesce(b."OutputAmount",1)))::numeric as requ_qty,
//						   bc.id as bc_id,
//						   bc."Material_id"::text as my_key,
//						   '' as parent_key
//					from jr
//					join bom b on b."Material_id" = jr.prod_pk
//					join bom_comp bc on bc."BOM_id" = b.id
//
//					union all
//
//					-- children: parent requ_qty 승계
//					select t.lvl+1,
//						   t.jr_id,
//						   t.mat_id                  as parent_mat_id,
//						   bc."Material_id"          as mat_id,
//						   t.prod_pk,
//						   t.order_qty,
//						   bc."Amount"::numeric,
//						   coalesce(b."OutputAmount",1),
//						   (bc."Amount" / coalesce(b."OutputAmount",1))::numeric,
//						   (t.requ_qty * (bc."Amount" / coalesce(b."OutputAmount",1)))::numeric as requ_qty,
//						   bc.id,
//						   t.my_key || '-' || bc."Material_id"::text,
//						   t.my_key
//					from bom_tree t
//					join bom b on b."Material_id" = t.mat_id
//					join bom_comp bc on bc."BOM_id" = b.id
//				)
//				select t.jr_id,
//					   t.lvl,
//					   case when t.lvl > 1 then t.parent_key end as parent_key,
//					   t.mat_id,
//					   t.parent_mat_id,
//					   fn_code_name('mat_type', mg."MaterialType") as mat_type,
//					   m."Name" as mat_name,
//					   m."Code" as mat_code,
//					   t.qty,
//					   t.produced_qty,
//					   t.bom_ratio::numeric(15,7) as bom_ratio,
//					   t.requ_qty,   -- ✅ 실제 필요한 수량
//					   u."Name" as unit,
//					   t.bc_id
//				from bom_tree t
//				join material m on m.id = t.mat_id
//				left join unit u on u.id = m."Unit_id"
//				left join mat_grp mg on m."MaterialGroup_id" = mg.id
//				order by t.jr_id, t.lvl, t.mat_id;
//
//        		""";
//
//        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
//
//        return items;
//	}

	// 합산 버전(반제품, 자제등 소요되는 품목 모두 보여줌)
	public List<Map<String, Object>> bomDetailList(String jr_pks, String data_date) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("jr_pks", jr_pks);
		paramMap.addValue("data_date", Date.valueOf(data_date));

		String sql = """
			 with recursive
				 A as (
					 select unnest(string_to_array(:jr_pks, ','))::int as jr_pk
				 ),
				 jr as (
					 select jr.id as jr_id,
							jr."Material_id" as prod_pk,
							jr."OrderQty" as order_qty
					 from A
					 join job_res jr on jr.id = A.jr_pk
					 where jr."MaterialProcessInputRequest_id" is null
					   and jr."State" in ('ordered','working')
				 ),
				 bom_tree as (
					 -- root: job_res → bom → bom_comp
					 select 1 as lvl,
							jr.jr_id,
							b."Material_id"        as parent_mat_id,
							bc."Material_id"       as mat_id,
							jr.prod_pk,
							jr.order_qty,
							bc."Amount"::numeric   as qty,
							coalesce(b."OutputAmount",1) as produced_qty,
							(bc."Amount" / coalesce(b."OutputAmount",1))::numeric as bom_ratio,
							(jr.order_qty * (bc."Amount" / coalesce(b."OutputAmount",1)))::numeric as requ_qty
					 from jr
					 join bom b on b."Material_id" = jr.prod_pk
					 join bom_comp bc on bc."BOM_id" = b.id
				 
					 union all
				 
					 -- children: parent requ_qty 승계
					 select t.lvl+1,
							t.jr_id,
							t.mat_id                  as parent_mat_id,
							bc."Material_id"          as mat_id,
							t.prod_pk,
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
							fn_code_name('mat_type', mg."MaterialType") as mat_type_name,
							mg."Name" as mat_group_name,
							m."Code" as mat_code,
							m."Name" as mat_name,
							u."Name" as unit_name,
							sum(t.requ_qty) as requ_qty,   -- 같은 자재 합산
							m."CurrentStock" as cur_stock,
							m."AvailableStock" as available_stock,
							coalesce(m."ProcessSafetyStock",0) as proc_safety_stock
					 from bom_tree t
					 join material m on m.id = t.mat_id
					 join mat_grp mg on mg.id = m."MaterialGroup_id"
					 left join unit u on u.id = m."Unit_id"
					 group by t.mat_id, mg."MaterialType", mg."Name",
							  m."Code", m."Name", u."Name",
							  m."CurrentStock", m."AvailableStock", m."ProcessSafetyStock"
				 ),
				 S as (
					 select R.mat_id,
							coalesce(sum(case when sh."HouseType"='material' then mh."CurrentStock" end),0) as material_stock,
							coalesce(sum(case when sh."HouseType"='process' then mh."CurrentStock" end),0) as process_stock
					 from R
					 join mat_in_house mh on mh."Material_id" = R.mat_id
					 join store_house sh on sh.id = mh."StoreHouse_id"
					 where sh."HouseType" in ('material','process')
					 group by R.mat_id
				 )
				 select R.mat_id,
						R.mat_type_name,
						R.mat_group_name,
						R.mat_code,
						R.mat_name,
						R.unit_name,
						R.requ_qty,
						S.material_stock,
						S.process_stock,
						R.proc_safety_stock,
						R.cur_stock,
						greatest(0, R.requ_qty + (coalesce(R.proc_safety_stock,0) - greatest(0, S.process_stock))) as input_req_qty
				 from R
				 left join S on S.mat_id = R.mat_id
				 order by R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name;
				 
        		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
		
	// 투입요청내역 조회 
	public List<Map<String, Object>> matProcInputList (Integer req_pk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("req_pk", req_pk);
		
        String sql = """
        		with R as (
                    select  mi."MaterialProcessInputRequest_id" as req_pk
                    , mi."Material_id" as mat_pk
                    , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                    , mg."Name" as mat_group_name
                    , m."Code" as mat_code
                    , m."Name" as mat_name 	
                    , u."Name" as unit_name
                    , mi."RequestQty" as req_qty
                    , m."CurrentStock" as cur_stock
                    ,coalesce( m."ProcessSafetyStock",0) as proc_safety_stock
                    , mi."MaterialStoreHouse_id", mi."ProcessStoreHouse_id"
                    , fn_code_name('mat_proc_input_state', mi."State") as state_name
                    from mat_proc_input mi
                    inner join material m on m.id = mi."Material_id"
                    inner join mat_grp mg on mg.id = m."MaterialGroup_id"	
                    left join unit u on u.id = m."Unit_id"
                    where mi."MaterialProcessInputRequest_id" = :req_pk
                ), S as (
                    select R.mat_pk
                    ,coalesce( sum(case when sh."HouseType" = 'material' then mh."CurrentStock" end),0) as material_stock
                    ,coalesce( sum(case when sh."HouseType" = 'process' then mh."CurrentStock" end),0) as process_stock
                    from R 
                    inner join mat_in_house mh on mh."Material_id" = R.mat_pk 
                    inner join store_house sh on sh.id = mh."StoreHouse_id"
                    where sh."HouseType" in ('material', 'process')
                    group by R.mat_pk
                )
                select R.mat_pk, R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
                , R.req_pk
                , R.req_qty
                , R.state_name
                , R.unit_name
                , S.material_stock
                , S.process_stock 
                , R.proc_safety_stock
                from R 
                left join S on S.mat_pk = R.mat_pk
                order by R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
        		""";
  		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
}
