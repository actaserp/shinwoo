package mes.app.production.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.entity.MaterialLot;
import mes.domain.entity.MatLotCons;
import mes.domain.entity.StoreHouse;
import mes.domain.repository.MatLotConsRepository;
import mes.domain.repository.MatLotRepository;
import mes.domain.repository.StorehouseRepository;
import mes.domain.services.SqlRunner;

@Service
public class ProductionResultService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	StorehouseRepository storehouseRepository;
	
	@Autowired
	MatLotConsRepository matLotConsRepository;
	
	@Autowired
	MatLotRepository matLotRepository;
	
	public void add_jobres_defectqty_inout(Integer jrPk, int id) {
		
		List<StoreHouse> sh = this.storehouseRepository.findByHouseType("defect");
		Integer defectHousePk = null;
		if (sh.size() > 0) {
			defectHousePk = sh.get(0).getId();
		} else {
			return;
		}
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		dicParam.addValue("housePk", defectHousePk);
		dicParam.addValue("userId", id);
		
		String sql = """
				 insert into mat_inout ("Material_id","StoreHouse_id", "InoutDate", "InoutTime", "InOut", "InputType"
               , "InputQty", "Description", "SourceDataPk", "SourceTableName", "State", _status, _created, _creater_id)
               select jr."Material_id"
               , :housePk
               , now()::date as "InoutDate"
               , now()::time as "InoutTime"
               ,'in' as "InOut"
               ,'produced_in' as "InputType"
               , jrd."DefectQty" as "InputQty"
               , dt."Name" as "Description"
               , jrd.id as "SourceDataPk"
               , 'job_res_defect' as "SourceTableName"
               , 'confirmed' as status
               , 'a' as _status
               , now() as _created
               , :userId as _creater_id
               from job_res_defect jrd 
               inner join job_res jr on jr.id=jrd."JobResponse_id"
               left join defect_type dt on dt.id = jrd."DefectType_id" 
               where jrd."DefectQty" > 0 
               and jrd."JobResponse_id" = :jrPk
				""";
		
		this.sqlRunner.execute(sql, dicParam);
	}

	public void delete_jobres_defectqty_inout(Integer jrPk) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("jrPk", jrPk);
		
		String sql = """
				delete from mat_inout 
		        where "SourceTableName"='job_res_defect' 
		        and "SourceDataPk" in (select id 
	            from job_res_defect 
	            where "JobResponse_id" = :jrPk)
				""";
		this.sqlRunner.execute(sql, dicParam);
		
	}

	public List<Map<String, Object>> get_chasu_bom_mat_qty_list(int id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		
		String sql = """
	       		with mp as(
		        select 
		        "Material_id"
		        , (COALESCE("GoodQty",0)+COALESCE("DefectQty",0)+COALESCE("ScrapQty",0)+COALESCE("LossQty",0)) as prod_qty
		        , "ProductionDate"
		        from mat_produce
		         where id = :id
		        ), bom1 as (
		        select b1.id as bom_pk, b1."Material_id" as prod_pk
		        , b1."OutputAmount" as produced_qty
		        , mp.prod_qty
		        , row_number() over(partition by b1."Material_id" order by b1."Version" desc) as g_idx
		        from bom b1
		         inner join mp on mp."Material_id"=b1."Material_id"
		        where b1."BOMType" = 'manufacturing' and mp."ProductionDate" between b1."StartDate" and b1."EndDate"  
		        ), BT as (
		        select 
		        bc."Material_id" as mat_pk
		        , bom1.produced_qty
		        , bc."Amount" as quantity 
		        , bc."Amount" / bom1.produced_qty as bom_ratio
		        , bc."Amount" / bom1.produced_qty * bom1.prod_qty as chasu_bom_qty 
		        from bom_comp bc 
		        inner join bom1 on bom1.bom_pk=bc."BOM_id"
		        where bom1.g_idx = 1
		        )
		        select 
		        BT.mat_pk
		        , mg."MaterialType" as mat_type
		        , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
		        , mg."Name" as mat_group_name
		        , m."Code" as mat_code
		        , m."Name" as mat_name
		        , u."Name" as unit_name
		        , BT.bom_ratio
		        , BT.chasu_bom_qty
		        , coalesce(m."LotUseYN",'N') as "lotUseYn"
		        from BT
		        inner join material m on m.id=BT.mat_pk
		        left join mat_grp mg on mg.id=m."MaterialGroup_id"
		        left join unit u on u.id=m."Unit_id"
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

    public void calculate_balance_mat_lot_with_job_res(int id) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("id", id);

        String sql = """
                      with ll as(
                      select 
                      ml.id as ml_id
                      from job_res jr  
                      inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id"=jr."MaterialProcessInputRequest_id"
                      inner join mat_lot ml on ml.id = mpi."MaterialLot_id" 
                      where jr.id = :id
                      ), ss as( select 
                      ll.ml_id, sum(mlc."OutputQty") as out_sum 
                      from ll 
                      left join mat_lot_cons mlc on ll.ml_id= mlc."MaterialLot_id" 
                      group by ll.ml_id
                      ), T as(
                      select 
                      ss.ml_id, coalesce(ss.out_sum,0) as out_sum, ml."InputQty" 
                      from ss
                      inner join mat_lot ml on ml.id=ss.ml_id
                      )
                      update mat_lot set "OutQtySum" = T.out_sum
                      , "CurrentStock" = mat_lot."InputQty"-T.out_sum
                      from T 
                      where T.ml_id = mat_lot.id
                """;

        this.sqlRunner.execute(sql, dicParam);
    }

    public void delete_mlc_and_rebalance_ml(int id) {
        List<MatLotCons> mcList = this.matLotConsRepository.findBySourceTableNameAndSourceDataPk("mat_produce", id);

        for (int i = 0; i < mcList.size(); i++) {
            MaterialLot ml = this.matLotRepository.getMatLotById(mcList.get(i).getMaterialLotId());
            Integer mId = ml.getId();
            this.matLotConsRepository.deleteById(mcList.get(i).getId());

            MapSqlParameterSource dicParam = new MapSqlParameterSource();
            dicParam.addValue("mId", mId);

            String sql = """
                             with SS as (
                             select 
                             ml.id as ml_id, sum("OutputQty") as out_qty_sum
                             from mat_lot_cons mlc 
                             inner join mat_lot ml on ml.id = mlc."MaterialLot_id"   
                             where ml.id= :mId
                             group by ml.id
                             )        
                             update mat_lot set 
                              "CurrentStock" = mat_lot."InputQty"-COALESCE(ss.out_qty_sum,0)
                              , "OutQtySum" = COALESCE(ss.out_qty_sum,0)
                              , _modified = now()
                             from ss
                             where ss.ml_id = mat_lot.id
                    """;


            this.sqlRunner.execute(sql, dicParam);
        }
    }

    public void calculate_balance_mat_lot_with_mat_prod(int id) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("mpId", id);

        String sql = """
                   with MS as (
                 	    select 
                      ml.id, sum(mlc."OutputQty") as "OutQtySum"
                      from mat_lot_cons mlc 
                      inner join mat_lot ml on ml.id = mlc."MaterialLot_id"
                      inner join mat_produce mp on mp.id= mlc."SourceDataPk" and mlc."SourceTableName" ='mat_produce'
                      where mlc."SourceDataPk"= :mpId
                      group by ml.id 
                      )
                      update mat_lot set 
                      "CurrentStock" = mat_lot."InputQty"-COALESCE(MS."OutQtySum",0)
                      , "OutQtySum" = MS."OutQtySum"
                      , _modified = now()
                      from MS
                      where MS.id = mat_lot.id
                """;

        this.sqlRunner.execute(sql, dicParam);
    }

    public List<Map<String, Object>> getProdResult(String dateFrom, String dateTo, String shiftCode,
                                                   String matType, String isIncludeComp, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("dateFrom", dateFrom);
        dicParam.addValue("dateTo", dateTo);
        dicParam.addValue("shiftCode", shiftCode);
        dicParam.addValue("matType", matType);
        dicParam.addValue("isIncludeComp", isIncludeComp);
		dicParam.addValue("spjangcd", spjangcd);

        String sql = """
				select
				  jr.id,
				  jr."WorkOrderNumber" as order_num,
				  to_char(jr."ProductionDate",'yyyy-mm-dd') as prod_date,
				  jr."ShiftCode" as shift_code, sh."Name" as shift_name,
				  m."Code" as mat_code, m."Name" as mat_name,
				  u."Name" as unit,
				  wc.id as workcenter_id, wc."Name" as workcenter,
				  e.id  as equipment_id, e."Name" as equipment,
				  jr."State" as state, fn_code_name('job_state', jr."State") as job_state,
				  jr."Description" as description,
				  jr."OrderQty" as order_qty,
				   
				  /* 필요투입량(= 다음 공정이 이 품목을 필요로 하는 수량) */
				  coalesce(next_need.cons_qty, 0) as cons_qty,
				   
				  jr."GoodQty" as good_qty, jr."DefectQty" as defect_qty,
				  jr."WorkIndex" as work_idx,
				   
				  jr."Material_id" as product_id,
				  -- 공정명은 워크센터 기준
				  wc."Process_id" as process_id,
				  pr_wc."Code" as process_code,
				  pr_wc."Name" as process_name,
				   
				  -- 공정순서: 라우팅에 동일 공정을 찾아서 매핑
				  rp_ord."ProcessOrder" as process_order,
				  coalesce(rp_ord."ProcessOrder", 999) as process_order_sort
				   
				from job_res jr
				left join material     m   on m.id   = jr."Material_id"
				left join unit         u   on u.id   = m."Unit_id"
				left join work_center  wc  on wc.id  = jr."WorkCenter_id"
				left join equ          e   on e.id   = jr."Equipment_id"
				left join shift        sh  on sh."Code" = jr."ShiftCode"
				   
				-- 공정명: 워크센터 → 공정
				left join process pr_wc
				  on pr_wc.id = wc."Process_id"
				   
				-- 현재 공정의 공정순서
				left join routing_proc rp_ord
				  on rp_ord."Routing_id" = jr."Routing_id"
				 and rp_ord."Process_id" = wc."Process_id"
				   
				-- ▼ 다음(즉시 다음 순서) 공정이 요구하는 '현 품목'의 필요수량 합
				left join lateral (
				  with nxt_po as (
				    select min(rp2."ProcessOrder") as po
				    from routing_proc rp2
				    where rp2."Routing_id" = jr."Routing_id"
				      and rp2."ProcessOrder" > coalesce(rp_ord."ProcessOrder", 999)
				  )
				  select
				    sum(bpc."Amount" * jr_next."OrderQty") as cons_qty
				  from nxt_po
				  join job_res jr_next
				    on jr_next."WorkOrderNumber" = jr."WorkOrderNumber"
				   and jr_next.spjangcd         = jr.spjangcd
				  join work_center wc_next
				    on wc_next.id = jr_next."WorkCenter_id"
				  join routing_proc rp_next
				    on rp_next."Routing_id" = jr_next."Routing_id"
				   and rp_next."Process_id" = wc_next."Process_id"
				   and rp_next."ProcessOrder" = nxt_po.po               -- 바로 다음 단계만
				  join bom_proc_comp bpc
				    on bpc."Routing_id" = jr."Routing_id"
				   and bpc."Process_id" = wc_next."Process_id"          -- 소비(다음) 공정의 BOM
				   and bpc."Material_id" = jr."Material_id"             -- 현 공정 산출(= 현 품목)이 다음 공정의 투입 자재로 쓰임
				   and bpc."Product_id"  = jr_next."Material_id"        -- 다음 공정 산출품(BOM의 대상)
				) as next_need on true
				   
				where jr."ProductionDate" between cast(:dateFrom as date) and cast(:dateTo as date)
				  and jr.spjangcd = :spjangcd
			""";
        if (StringUtils.isEmpty(matType) == false) sql += "and mg.\"MaterialType\" = :matType ";
        if (!shiftCode.equals("")) sql += " and jr.\"ShiftCode\" = :shiftCode ";
        if (isIncludeComp.equals("false")) sql += " and jr.\"State\" != 'finished' ";

        sql += " order by jr.\"WorkOrderNumber\", process_order_sort, jr.id ";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public Map<String, Object> getProdResultDetail(Integer jrPk) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("jrPk", jrPk);

        String sql = """
                select distinct jr.id
                         , jr."WorkOrderNumber" as order_num
                         , jr."LotNumber" as lot_num
                         , jr."State" as state
                         , fn_code_name('job_state', jr."State") as job_state
                         , jr."WorkIndex" as work_idx
                         , m.id as mat_pk, m."Code" as mat_code, m."Name" as mat_name
                         , m."LotSize"  as lot_size
                         , u."Name" as unit
                         , jr."OrderQty" as order_qty
                         , coalesce(jr."GoodQty", 0) as good_qty
                         , coalesce(jr."DefectQty", 0) as defect_qty
                         , coalesce(jr."LossQty", 0) as loss_qty
                         , coalesce(jr."ScrapQty", 0) as scrap_qty
                         , to_char(jr."ProductionDate", 'yyyy-mm-dd') as prod_date
                         , to_char(jr."StartTime", 'hh24:mi') as start_time
                         , jr."EndDate" as end_date
                         , to_char(jr."StartTime", 'yyyy-mm-dd') as start_date
                         , to_char(jr."EndTime", 'hh24:mi') as end_time
                         , jr."ShiftCode" as shift_code, sh."Name" as shift_name
                         , wc.id as workcenter_id, wc."Name" as workcenter_name
                         , e.id as equipment_id, e."Name" as equipment_name
                         , jr."Description" as description 
                         , m."ValidDays"
                         , coalesce(next_need.cons_qty, 0) as cons_qty
                      from job_res jr 
                      left join material m on m.id = jr."Material_id"
                      left join unit u on u.id = m."Unit_id"
                      left join work_center wc on wc.id = jr."WorkCenter_id"
                      left join equ e on e.id = jr."Equipment_id"
					  left join shift sh on sh."Code" = jr."ShiftCode"
					  left join mat_produce mp on mp."JobResponse_id" = jr.id
					  left join process pr_wc
						  on pr_wc.id = wc."Process_id"
						   
						-- 현재 공정의 공정순서
						left join routing_proc rp_ord
						  on rp_ord."Routing_id" = jr."Routing_id"
						 and rp_ord."Process_id" = wc."Process_id"
					  left join lateral (
						  with nxt_po as (
							select min(rp2."ProcessOrder") as po
							from routing_proc rp2
							where rp2."Routing_id" = jr."Routing_id"
							  and rp2."ProcessOrder" > coalesce(rp_ord."ProcessOrder", 999)
						  )
						  select
							sum(bpc."Amount" * jr_next."OrderQty") as cons_qty
						  from nxt_po
						  join job_res jr_next
							on jr_next."WorkOrderNumber" = jr."WorkOrderNumber"
						   and jr_next.spjangcd         = jr.spjangcd
						  join work_center wc_next
							on wc_next.id = jr_next."WorkCenter_id"
						  join routing_proc rp_next
							on rp_next."Routing_id" = jr_next."Routing_id"
						   and rp_next."Process_id" = wc_next."Process_id"
						   and rp_next."ProcessOrder" = nxt_po.po               -- 바로 다음 단계만
						  join bom_proc_comp bpc
							on bpc."Routing_id" = jr."Routing_id"
						   and bpc."Process_id" = wc_next."Process_id"          -- 소비(다음) 공정의 BOM
						   and bpc."Material_id" = jr."Material_id"             -- 현 공정 산출(= 현 품목)이 다음 공정의 투입 자재로 쓰임
						   and bpc."Product_id"  = jr_next."Material_id"        -- 다음 공정 산출품(BOM의 대상)
						) as next_need on true
                       where jr.id = :jrPk
                """;

        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        return item;
    }

    public List<Map<String, Object>> getDefectList(Integer jrPk, Integer workcenterId) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jrPk", jrPk);
        dicParam.addValue("workcenterId", workcenterId);

        String sql = """
                 with TOT as (
                          select jrd.id as jrd_id
                          , jrd."DefectQty" as defect_qty
                          , jrd."DefectType_id"  as defect_id
                          , jrd."Description" as defect_remark
                          from job_res_defect jrd 
                          where jrd."JobResponse_id" = :jrPk
                       ), a as(
                         select 
                         jr."WorkCenter_id"
                         , wc."Process_id"
                         , pdt."DefectType_id" as defect_id
                         , dt."Name" as defect_type
                         , coalesce( TOT.defect_qty,0) as defect_qty
                         , TOT.jrd_id
                         , TOT.defect_remark
                         from job_res jr 
                         left join work_center wc on wc.id=jr."WorkCenter_id"  
                         left join proc_defect_type pdt on pdt."Process_id" =wc."Process_id" 
                         inner join defect_type dt on dt.id = pdt."DefectType_id" 
                         left join TOT on TOT.defect_id=dt.id
                         where jr.id = :jrPk
                         )
                         select * from a
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public List<Map<String, Object>> getChasuList(Integer jrPk) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jrPk", jrPk);

        String sql = """
			 select id
				  , "LotIndex" as chasu
				  , "LotNumber" as lot_no
				  , "GoodQty" as good_qty
				  , "DefectQty" as defect_qty
				  , "LossQty" as loss_qty
				  , "ScrapQty" as scrap_qty
				  , to_char("EndTime", 'YYYY-MM-DD HH24:MI') as end_time
				  , to_char("StartTime", 'YYYY-MM-DD HH24:MI') as start_time
				  , case
					  when "_modified" is null then to_char("_created", 'YYYY-MM-DD HH24:MI')
					  else to_char("_modified", 'YYYY-MM-DD HH24:MI')
					end as input_time
			 from mat_produce
			 where "JobResponse_id" = :jrPk
			 order by "LotIndex"
			""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public List<Map<String, Object>> getInputLotList(Integer jrPk) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jrPk", jrPk);

        String sql = """
                with AA as (
                         select 
                         ml."LotNumber"
                         , sum(mlc."OutputQty") as "OutputQty" 
                         from mat_produce mp 
                         inner join job_res jr on jr.id = mp."JobResponse_id"
                         inner join mat_lot_cons mlc on mlc."SourceDataPk" = mp.id and mlc."SourceTableName" ='mat_produce'   
                         inner join mat_lot ml on ml.id = mlc."MaterialLot_id" 
                         where jr.id= :jrPk group by ml."LotNumber" 
                         ), R as (
                             select  mpir.id as mpir_id
                             , mpi.id as mpi_id
                             , mpi."Material_id" as mat_pk
                             , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                             , mg."Name" as mat_group_name
                             , m."Code" as mat_code
                             , m."Name" as mat_name 
                             , u."Name" as unit_name
                             , mpi."RequestQty" as req_qty
                             , mpi."InputQty" 
                             , to_char(mpi."InputDateTime",'yyyy-MM-dd') as "InputDateTime"
                             , ml."LotNumber"
                             , ml."CurrentStock" as cur_stock
                             , m."ProcessSafetyStock" as proc_safety_stock
                             , mpi."MaterialStoreHouse_id"
                             , mpi."ProcessStoreHouse_id"
                             , mpi."State"
                             , fn_code_name('mat_proc_input_state', mpi."State") as state_name
                             , sh."Name" as "StoreHouseName"
                             from job_res jr 
                             inner join mat_proc_input_req mpir on mpir.id = jr."MaterialProcessInputRequest_id" 
                             inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id" =mpir.id
                             inner join material m on m.id = mpi."Material_id"
                             inner join mat_grp mg on mg.id = m."MaterialGroup_id"
                             left join unit u on u.id = m."Unit_id"
                             left join mat_lot ml on ml.id = mpi."MaterialLot_id"
                             left join store_house sh on sh.id=ml."StoreHouse_id"
                             where jr.id =  :jrPk
                          )
                          select R.mat_pk, R.mat_type_name, R.mat_group_name, R.mat_code, R.mat_name
                          , R.mpir_id
                          , R.mpi_id
                          , R.req_qty
                          , R."InputQty" 
                          , R."LotNumber" as lot_number
                          , R.state_name
                          , R.unit_name
                          , R.cur_stock
                          , R."State" 
                          , R."InputDateTime" as start_date
                          , R."StoreHouseName"
                          , COALESCE(AA."OutputQty", 0) as consumed_qty
                          from R 
                          left join AA on AA."LotNumber" = R."LotNumber"
                          order by R."InputDateTime", R."LotNumber"
                	""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

	public boolean hasProcBom(Integer jrPk, Integer procId) {
		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("jrPk", jrPk);
		p.addValue("procId", procId);

		String sql = """
        select 1
        from job_res jr
        join work_center wc on wc.id = jr."WorkCenter_id"
        join bom_proc_comp bpc
          on bpc."Routing_id" = jr."Routing_id"
         and bpc."Product_id" = jr."Material_id"
         and (bpc."Process_id" = coalesce(:procId, wc."Process_id"))
         and (bpc.spjangcd = jr.spjangcd or bpc.spjangcd is null)
        where jr.id = :jrPk
        limit 1
    """;

		List<Map<String,Object>> rows = this.sqlRunner.getRows(sql, p);
		return !rows.isEmpty();
	}


	public List<Map<String, Object>> getConsumedListByProc(
			Integer jrPk, Integer prodPk, Integer procId, String prodDate) {

		MapSqlParameterSource p = new MapSqlParameterSource();
		p.addValue("jrPk", jrPk);
		p.addValue("prodPk", prodPk);
		p.addValue("procId", procId);

		String sql = """
			with sel as (
			 select
				 jr.id                 as jr_pk,
				 jr."Routing_id"       as routing_id,
				 jr."Material_id"      as product_id,
				 wc."Process_id"       as process_id,
				 jr."OrderQty"         as order_qty,
				 jr."WorkOrderNumber"  as workorder_num,
				 jr.spjangcd           as spjangcd
			 from job_res jr
			 join work_center wc on wc.id = jr."WorkCenter_id"
			 where jr.id = :jrPk
		   ),
		   /* 현재 공정 순서와 “바로 다음 공정” 순서 구하기 */
		   cur_po as (
			 select rp."ProcessOrder" as po
			 from routing_proc rp join sel s
			   on rp."Routing_id" = s.routing_id
			  and rp."Process_id" = coalesce(:procId, s.process_id)
			 limit 1
		   ),
		   nxt_po as (
			 select min(rp2."ProcessOrder") as po
			 from routing_proc rp2, sel s, cur_po c
			 where rp2."Routing_id" = s.routing_id
			   and rp2."ProcessOrder" > c.po
		   ),
		   
		   /* 다음 공정이 요구하는 '현 품목' 수량(cons_qty) */
		   next_need as (
			  select
				sum(bpc."Amount" * jr_next."OrderQty") as cons_qty
			  from sel s
			  -- 콤마 대신 명시적 조인으로 np를 스코프 안에 둠
			  join nxt_po np on true
			  join job_res jr_next
				on jr_next."WorkOrderNumber" = s.workorder_num
			   and jr_next.spjangcd          = s.spjangcd
			  join work_center wc_next
				on wc_next.id = jr_next."WorkCenter_id"
			  join routing_proc rp_next
				on rp_next."Routing_id"   = jr_next."Routing_id"
			   and rp_next."Process_id"   = wc_next."Process_id"
			   and rp_next."ProcessOrder" = np.po     -- ← 이제 유효
			  join bom_proc_comp bpc
				on bpc."Routing_id"  = s.routing_id
			   and bpc."Process_id"  = wc_next."Process_id"   -- 소비(다음) 공정의 BOM
			   and bpc."Material_id" = s.product_id           -- 현 공정 산출품이 다음 공정 투입자재로
			   and bpc."Product_id"  = jr_next."Material_id"  -- 다음 공정 산출품
			   and (bpc.spjangcd = s.spjangcd or bpc.spjangcd is null)
			),
				
		   /* 현 공정의 투입 BOM (공정별) */
		   boms as (
			 select
				 bpc.id            as bpc_id,
				 bpc."Material_id" as mat_pk,
				 bpc."Amount"      as amount_per
			 from bom_proc_comp bpc
			 join sel s
			   on s.routing_id = bpc."Routing_id"
			  and bpc."Product_id" = s.product_id
			  and bpc."Process_id" = coalesce(:procId, s.process_id)  -- 단순화
			  and (bpc.spjangcd = s.spjangcd or bpc.spjangcd is null)
		   ),
		   
		   /* plan_qty: cons_qty 우선, 없으면 order_qty */
		   plan as (
			 select coalesce(nullif(nn.cons_qty, 0), s.order_qty) as plan_qty
			 from sel s
			 left join next_need nn on true
		   )
		   
		   select
			 b.mat_pk,
			 mg."MaterialType"                           as mat_type,
			 fn_code_name('mat_type', mg."MaterialType") as mat_type_name,
			 mg."Name"                                   as mat_group_name,
			 m."Code"                                    as mat_code,
			 m."Name"                                    as mat_name,
			 m."LotSize"                                 as lot_size,
			 u."Name"                                    as unit,
			 b.amount_per                                as amount_per,                         -- 산출 1당 필요수량
			 round( (b.amount_per * p.plan_qty)::numeric ) as bom_consumed,                    -- ✅ 계획 투입량
			 p.plan_qty                                  as plan_qty,                           -- 참고용(표시/디버그)
			 coalesce((select cons_qty from next_need),0) as cons_qty_next,                     -- 참고용
			 coalesce(llc.consumed_qty,0)                as consumed_qty,
			 coalesce(mcc.mc_qty,0)                      as mc_qty,
			 coalesce(mmp.current_qty_sum,0)             as current_qty_sum,
			 sh."Name"                                   as storehouse_name,
			 mh."CurrentStock" as "currentStock",
			 coalesce(m."LotUseYN",'N')                  as "lotUseYn",
			 case when m."Useyn" = '1' then 'Y'
				  when m."Useyn" = '0' then 'N' else null end as useyn
		   from boms b
		   join sel s on true
		   join plan p on true
		   join material m  on m.id = b.mat_pk
		   left join unit u on u.id = m."Unit_id"
		   left join mat_grp mg on mg.id = m."MaterialGroup_id"
		   left join store_house sh on m."StoreHouse_id" = sh.id
		   left join mat_in_house mh on mh."Material_id" = m.id and mh."StoreHouse_id"  = m."StoreHouse_id"
		   left join (
			 select sum(mlc."OutputQty") as consumed_qty, ml."Material_id"
			 from job_res jr
			 join mat_produce mp   on mp."JobResponse_id" = jr.id and jr.id = :jrPk
			 join mat_lot_cons mlc on mlc."SourceDataPk" = mp.id and mlc."SourceTableName" = 'mat_produce'
			 join mat_lot ml       on ml.id = mlc."MaterialLot_id"
			 group by ml."Material_id"
		   ) llc on llc."Material_id" = b.mat_pk
		   left join (
			 select mc."Material_id" as mat_pk, sum(mc."ConsumedQty") as mc_qty
			 from mat_consu mc
			 where mc."JobResponse_id" = :jrPk
			 group by mc."Material_id"
		   ) mcc on mcc.mat_pk = b.mat_pk
		   left join (
			 select sum(mpi."RequestQty") as current_qty_sum, mpi."Material_id"
			 from mat_proc_input mpi
			 join job_res jr on jr."MaterialProcessInputRequest_id" = mpi."MaterialProcessInputRequest_id"
			 join mat_lot ml on ml.id = mpi."MaterialLot_id"
			 where jr.id = :jrPk
			 group by mpi."Material_id"
		   ) mmp on mmp."Material_id" = m.id
		   order by mat_code, mat_name;
				                           
        """;

		return this.sqlRunner.getRows(sql, p);
	}


	public List<Map<String, Object>> getConsumedListFirst(Integer jrPk, Integer prodPk, String prodDate) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jrPk", jrPk);
        dicParam.addValue("prodPk", prodPk);
        dicParam.addValue("prodDate", prodDate);

        String sql = """
                with bom1 as (
                            select 
                            b1.id as bom_pk
                            , b1."Material_id" as prod_pk
                            , b1."OutputAmount" as produced_qty
                            , jr."OrderQty" as order_qty
                            , row_number() over(partition by b1."Material_id" order by b1."Version" desc) as g_idx
                            from bom b1
                             inner join job_res jr on jr."Material_id"=b1."Material_id" and jr.id= :jrPk
                            where b1."BOMType" = 'manufacturing' and jr."ProductionDate" between b1."StartDate" and b1."EndDate"  
                            ), BT as (
                            select 
                            bc."Material_id" as mat_pk
                            , bom1.produced_qty
                            , bc."Amount" as quantity 
                            , bc."Amount" / bom1.produced_qty as bom_ratio
                            , bc."Amount" / bom1.produced_qty * bom1.order_qty as bom_requ_qty 
                            from bom_comp bc 
                            inner join bom1 on bom1.bom_pk=bc."BOM_id"
                            where bom1.g_idx=1
                            ), llc as (
                            select 
                            sum(mlc."OutputQty") as consumed_qty
                            , ml."Material_id" 
                            from job_res jr 
                            inner join mat_produce mp on mp."JobResponse_id" =jr.id and jr.id= :jrPk
                            inner join mat_lot_cons mlc on mlc."SourceDataPk" =mp.id and mlc."SourceTableName" ='mat_produce'
                            inner join mat_lot ml on ml.id = mlc."MaterialLot_id" 
                            group by ml."Material_id" 
                            ), MCC as (
                                select 
                                mc."Material_id" as mat_pk
                                , sum(mc."ConsumedQty") mc_qty 
                                from mat_consu mc 
                                where mc."JobResponse_id"= :jrPk group by mc."Material_id"
                            ), MMP as (
                                select 
                                sum(ml."CurrentStock") as current_qty_sum
                                , mpi."Material_id"
                                from mat_proc_input mpi
                                inner join job_res jr on jr."MaterialProcessInputRequest_id" = mpi."MaterialProcessInputRequest_id" 
                                inner join mat_lot ml on ml.id = mpi."MaterialLot_id"
                                where jr.id=:jrPk
                                group by mpi."Material_id"
                            )
                            select 
                            BT.mat_pk
                            , mg."MaterialType" as mat_type
                            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
                            , mg."Name" as mat_group_name
                            , m."Code" as mat_code
                            , m."Name" as mat_name
                            , m."LotSize" as lot_size
                            , mh."CurrentStock" as "currentStock"
                            , u."Name" as unit
                            , BT.bom_ratio
                            , round(BT.bom_requ_qty::numeric) as bom_consumed
                            , COALESCE(llc.consumed_qty,0) as consumed_qty
                            , sh."Name" as storehouse_name
                            , MCC.mc_qty
                            , COALESCE(MMP.current_qty_sum,0) as current_qty_sum
                            , coalesce(m."LotUseYN",'N') as "lotUseYn"
                            , CASE WHEN m."Useyn" = '1' THEN 'Y'
				                   WHEN m."Useyn" = '0' THEN 'N'
				                   ELSE NULL
				              END as useyn
                            from BT
                            inner join material m on m.id=BT.mat_pk
                            left join MCC on MCC.mat_pk=BT.mat_pk
                            left join mat_grp mg on mg.id=m."MaterialGroup_id"
                            left join unit u on u.id=m."Unit_id"
                            left join llc on llc."Material_id" = BT.mat_pk
                            left join store_house sh on m."StoreHouse_id" = sh.id
                            left join mat_in_house mh on mh."Material_id" = m.id and mh."StoreHouse_id"  = m."StoreHouse_id" 
                            left join MMP on MMP."Material_id" = m.id
                            where m."Useyn" = '0'
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public List<Map<String, Object>> getConsumedListSecond(Integer jrPk, Integer prodPk, String prodDate) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("jrPk", jrPk);
        dicParam.addValue("prodPk", prodPk);
        dicParam.addValue("prodDate", prodDate);

        String sql = """
                with A as (
                                select 
                                l."Material_id" as mat_id
                                , sum(lc."OutputQty") as lot_consumed
                                from job_res jr
                                inner join mat_produce mp on mp."JobResponse_id" = jr.id 
                                inner join mat_lot_cons lc on lc."SourceDataPk" = mp.id
                                inner join mat_lot l on l.id = lc."MaterialLot_id" 
                                where lc."SourceTableName" = 'mat_produce'
                                and jr.id = :jrPk
                                group by l."Material_id"
                            )
                            select m.id as mat_pk
                            , m."Name" as mat_name
                            , u."Name" as unit
                            , fn_unit_ceiling( bom.bom_ratio * , u."PieceYN" ) as bom_consumed
                            , A.lot_consumed
                            , A.lot_consumed as consumed
                            from tbl_bom_detail(cast(:prodPk as text), cast(to_char(cast(:prodDate as date),'YYYY-MM-DD') as text)) as bom
                            inner join material m on m.id = bom.mat_pk
                            left join unit u on u.id = m."Unit_id"
                            left join A on A.mat_id = m.id
                            where bom.b_level = 1
                            order by tot_order 
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    public List<Map<String, Object>> prodTestList(Integer jrPk, Integer testResultId) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);
        param.addValue("testResultId", testResultId);

        String sql = """
                	select ti.id, up."Name" as "CheckName", ti."ResultType" as "resultType"
                	, tim."SpecText" as "specText"
                	, to_char(tir."TestDateTime", 'YYYY-MM-DD') as "testDate"
                	, tir."JudgeCode", tir."InputResult" as "ctRemark" ,tir."CharResult" as "ntRemark" , ti."Name" as name 
                	, tir."Char1" as result1, tir."Char2" as result2
                	, tr.id as "testResultId", tr."TestMaster_id" as "testMasterId"
                	from test_item_result tir
                	inner join test_result tr on tr.id = tir."TestResult_id"
                	inner join test_mast tm on tm.id = tr."TestMaster_id" 
                	inner join test_item ti on tir."TestItem_id"  = ti.id 
                	inner join test_item_mast tim on ti.id = tim."TestItem_id" and tim."TestMaster_id" = tm.id
                	inner join user_profile up on tir."_creater_id"  = up."User_id" 
                	where tr."SourceTableName" = 'job_res' and tr."SourceDataPk" = :jrPk
                	and tr.id = :testResultId
                	order by ti.id
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }

    public List<Map<String, Object>> prodTestDefaultList() {

        String sql = """
                select ti.id, ti."Name" as name , ti."ResultType" as "resultType", tim."SpecText" as "specText", '' as result1, '' as result2 
                from test_item_mast tim 
                inner join test_mast tm on tim."TestMaster_id"  = tm.id 
                inner join test_item ti on tim."TestItem_id"  = ti.id
                where tm."Name"  = '제품검사'
                   """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);

        return items;
    }

    public Integer getTestMasterByItem(Integer jrPk) {
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);

        String sql = """
                    SELECT tmm."TestMaster_id" AS testMasterId
                            FROM job_res jr
                            INNER JOIN test_mast_mat tmm ON jr."Material_id" = tmm."Material_id"
                            WHERE jr.id = :jrPk
                            LIMIT 1
                """;

        List<Map<String, Object>> result = this.sqlRunner.getRows(sql, param);
        return result.isEmpty() ? null : (Integer) result.get(0).get("testMasterId");
    }


    public List<Map<String, Object>> prodTestListByTestMaster(Integer testMasterId) {
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("testMasterId", testMasterId);

        String sql = """
                    SELECT tm.id AS testMasterId, ti.id, ti."Name" AS name, ti."ResultType" AS "resultType",
                           tim."SpecText" AS "specText", '' AS result1, '' AS result2
                    FROM test_item_mast tim
                    INNER JOIN test_mast tm ON tim."TestMaster_id" = tm.id
                    INNER JOIN test_item ti ON tim."TestItem_id" = ti.id
                    WHERE tm.id = :testMasterId
                """;

        return this.sqlRunner.getRows(sql, param);
    }


    public List<Map<String, Object>> getMaterialProcessInputList(int jrPk, int matPk) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);
        param.addValue("matPk", matPk);

        String sql = """
                select  mpi.id  as mpi_id
                	  ,	mpi."RequestQty" as req_qty
                	  , mpi."InputQty" as input_qty
                	  , mpi."Material_id" as mat_pk
                	  , ml."CurrentStock" as curr_qty
                	  , ml.id as ml_id
                	  , ml."LotNumber"
                	  , ml."EffectiveDate" as eff_date
                from job_res jr 
                inner join mat_proc_input mpi on mpi."MaterialProcessInputRequest_id"  = jr."MaterialProcessInputRequest_id"
                inner join mat_lot ml on ml.id = mpi."MaterialLot_id" 
                where jr.id = :jrPk
                and mpi."Material_id" = :matPk
                order by ml."EffectiveDate"
                   """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);

        return items;
    }

    public Map<String, Object> getJobResponseGoodDefectQty(Integer jrPk) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);

        String sql = """
                select jr.id
                	  ,coalesce(sum(mp."GoodQty"),0) as good_qty
                	  ,coalesce(sum(mp."DefectQty"),0) as defect_qty
                from job_res jr 
                inner join mat_produce mp on mp."JobResponse_id" = jr.id 
                where jr.id = :jrPk
                group by jr.id
                """;

        Map<String, Object> items = this.sqlRunner.getRow(sql, param);

        return items;
    }

    public float getChasuDefectQty(Integer jrPk) {

        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("jrPk", jrPk);

        String sql = """
                select coalesce(sum(mp."DefectQty"),0) as defect_qty 
                from mat_produce mp 
                   			where mp."JobResponse_id" = :jrPk
                   		""";

        Map<String, Object> items = this.sqlRunner.getRow(sql, param);

        float qty = Float.parseFloat(items.get("defect_qty").toString());

        return qty;
    }
}
