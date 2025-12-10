package mes.app.shipment.service;

import java.util.List;
import java.util.Map;

import mes.domain.entity.ShipmentHead;
import mes.domain.repository.ShipmentHeadRepository;
import mes.domain.repository.SujuRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ShipmentListService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	mes.domain.repository.SujuRepository sujuRepository;

	public List<Map<String, Object>> getShipmentHeadList(String dateFrom, String dateTo, String compPk, String matGrpPk, String matPk, String keyword, String state, String company) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("compPk", compPk);
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("keyword", keyword);
		//String state = "shipped";
		paramMap.addValue("state", state);
		
		String sql = """
				select sh.id
		        , sh."Company_id" as company_id
                , c."Name" as company_name
		        , sh."ShipDate" as ship_date
		        , sh."TotalQty" as total_qty
	            , sh."TotalPrice" as total_price
	            , sh."TotalVat" as total_vat
	            , sh."Description" as description
                , sh."State" as state
                , fn_code_name('shipment_state', sh."State") as state_name
                , to_char(coalesce(sh."OrderDate",sh."_created") ,'yyyy-mm-dd') as order_date
                , sh."StatementIssuedYN" as issue_yn
                , sh."StatementNumber" as stmt_number
                , sh."IssueDate" as issue_date
                from shipment_head sh
                join company c on c.id = sh."Company_id"
                where sh."ShipDate"  between cast(:dateFrom as date) and cast(:dateTo as date)
				""";

		if (StringUtils.isEmpty(company) == false) {
			sql += " AND  c.\"Name\" LIKE :company ";
			paramMap.addValue("company", "%" + company + "%");
		}
		if (StringUtils.isEmpty(compPk)==false)  sql += " and sh.\"Company_id\" = cast(:compPk as Integer) ";
		if (StringUtils.isEmpty(state)==false)  sql += " and sh.\"State\" = :state ";
		if (StringUtils.isEmpty(matPk)==false || StringUtils.isEmpty(matGrpPk)==false || StringUtils.isEmpty(keyword)==false) {
			sql += """
					and exists ( select 1
        		    from shipment s
                    inner join material m on m.id = s."Material_id"
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    where s."ShipmentHead_id" = sh.id
					""";
			if (StringUtils.isEmpty(matPk)==false)  sql += " and s.\"Material_id\"  = cast(:matPk as Integer) ";
			if (StringUtils.isEmpty(matGrpPk)==false)  sql += " and mg.id  = cast(:matGrpPk as Integer) ";
			if (StringUtils.isEmpty(keyword)==false)  sql += " and ( m.\"Name\" ilike concat('%%',:keyword,'%%') or m.\"Code\" ilike concat('%%',:keyword,'%%')) ";

			sql += " )";
		}
		sql += """ 
		 		order by sh."ShipDate" desc, sh.id desc
		 		""";
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
	//단가는 기본적으로 수주면 수주때 지정단가 가져오고 (수주지정단가는 shipment에 저장되어있음. 수주가 수정되었다면 다를수있긴함.)
	//제품출하라면 품목의 최근단가를 가져온다.
	public List<Map<String, Object>> getShipmentItemList(String headId, Integer company_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("headId", headId);
		paramMap.addValue("companyId", company_id);

		String sql = """
				select s.id as ship_pk
				, s."Material_id" as mat_pk
				, mg."Name" as mat_grp_name
				, m."Code" as mat_code
				, m."Name" as mat_name
	            , m."UnitPrice" as mat_unit_price
	            , coalesce((s."OrderQty" * m."UnitPrice"), 0) as order_mat_price
				, u."Name" as unit_name
				, s."OrderQty" as order_qty
				, s."Qty" as ship_qty
				, s."Description" as description
				, sh."Company_id" as company_id
				, m.id as material_id
				--,case
				--	when s."SourceTableName" = 'product' then mcu."UnitPrice"
				--	else su."UnitPrice"
				--end as unit_price
				
				--단가
				,s."UnitPrice" as unit_price
				--공급가
				,s."Price" as price 
				-- 부가세
				,s."Vat" as vat
				
				,case
					when s."SourceTableName" = 'product' then 'N'
					else su."InVatYN"
				end as invatyn
			
				--,TRUNC((
				--                  CASE
				--                    WHEN s."SourceTableName" = 'product' THEN mcu."UnitPrice" * s."Qty"
				--                    WHEN su."InVatYN" = 'Y' THEN (su."UnitPrice" * (10.0 / 11)) * s."Qty"
				--                    ELSE su."UnitPrice" * s."Qty"
				--                  END
				--                )::numeric, 2) AS price
				--,case
				--	when s."SourceTableName" = 'product' then (mcu."UnitPrice" * s."Qty") * 0.1
				--	when su."InVatYN" = 'Y' then (su."UnitPrice" - (su."UnitPrice" * (10.0/11))) * s."Qty"
				--	else (su."UnitPrice" * s."Qty") * 0.1
				--end as vat
	            , m."VatExemptionYN" as vat_exempt_yn
	            , s."SourceDataPk" as src_data_pk
	            , s."SourceTableName" as src_table_name
	            , case when s."SourceTableName" = 'rela_data' 
	            		then '수주출하'
	            	   when s."SourceTableName" = 'product'	
						then '제품출하'
					   else '알수없음'
				end as shipment_flag
				, COALESCE(su."Standard", m."Standard1") as standard	 
				from shipment  s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id"
				left join unit u on u.id = m."Unit_id" 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"  
	            left join (	
				             			select distinct on ("Material_id") "Material_id", "UnitPrice"
				             			from mat_comp_uprice
				         				WHERE "Type" = '02'
				             				AND "Company_id" = :companyId
				             				AND "ApplyEndDate" > CURRENT_DATE
				             			order by "Material_id", "ApplyStartDate" desc
				         				) mcu on mcu."Material_id" = s."Material_id" and s."SourceTableName" = 'product'
				left join suju su on su.id = s."SourceDataPk"	
				where s."ShipmentHead_id" = cast(:headId as Integer)
	            order by m."Code", m."Name"
				""";
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

//	public void updateSujuShipmentCancel(Integer shId) {
//		sujuRepository.updateShipmentStateByShipmentId(shId, "cancelled");
//	}

	public void updateShipmentQantityByLotConsume (Integer sh_id, Integer shipment_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);
		paramMap.addValue("shipment_id", shipment_id);

		String sql = """
				with A as(
	            select
	            s.id, coalesce(sum(mlc."OutputQty"),0) as qty  
	            from shipment s  
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	            left join mat_lot_cons mlc on mlc."SourceTableName" ='shipment' and mlc."SourceDataPk" = s.id
	            where 1=1 
	            and sh.id = :sh_id
				""";

		if (shipment_id != null) {
			sql += " and s.id = :shipment_id ";
		}

		sql += """
				group by s.id),
				UPC as (
	            select
	            s.id
	            , s."Material_id"
	            , sh."Company_id"
	            , mcu."UnitPrice"
	            , m."VatExemptionYN"
	            from A
	            inner join shipment s on s.id = A.id
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id" 
	            inner join material m on m.id = s."Material_id" 
	            left join mat_comp_uprice mcu on mcu."Material_id"=s."Material_id" and mcu."Company_id"=sh."Company_id" and mcu."ApplyStartDate" <=now() and mcu."ApplyEndDate" > now()
	            where sh.id = :sh_id 
	        ), B as(        
	           select 
	           s.id
	           , A.qty
	           , UPC."UnitPrice" 
	           , (A.qty * UPC."UnitPrice") as "Price"
	           , case when UPC."VatExemptionYN"='Y' then 0 else (A.qty * UPC."UnitPrice"*0.1) end  as "Vat" 
	           , s."Material_id"
	           , UPC."Company_id"
	           from shipment s 
	             inner join shipment_head sh2 on sh2.id = s."ShipmentHead_id"
	             inner join A on A.id = s.id             
	             inner join UPC on UPC.id = s.id
	        )
	        update shipment set 
	         "Qty" = B.qty 
	         , "UnitPrice" = B."UnitPrice"
	         , "Price" =  B."Price"
	         , "Vat" = B."Vat"
	        from B
	        where shipment.id = B.id
				""";

		this.sqlRunner.execute(sql, paramMap);
	}

	// 출고헤더 기준으로 상태값 (출고상태)변경
	public void updateShipmentStateCancel (Integer searchId) {

		updateShipmentQantityByLotConsume(searchId, null);

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("searchId", searchId);

		String sql = """
				with A as(
				select 
		        sh.id as sh_id
		        , count(s.id) as s_count
		        , sum(s."Price") as "TotalPrice"
		        , sum(s."Vat") as "TotalVat"
		        from shipment s 
		        inner join shipment_head sh on sh.id=s."ShipmentHead_id"
		        where sh.id=:searchId
		        group by sh.id 
		        )
		        update 
		        shipment_head 
		        set "State" = 'ordered'
		        from A 
		        where id=A.sh_id
				""";

		this.sqlRunner.execute(sql, paramMap);
	}

	// 출고 취소 관련 수주를 찾아서 수주의 출하 상태 출고 전으로 를 변경한다.
	public void updateSujuShipmentStateCancel (Integer sh_id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("sh_id", sh_id);

		String sql = """
		        with A as(
		        select
		        s.id as shipment_id
		        ,sh.id as sh_id
		        , rd."DataPk1" as suju_id
		        , sj."State"
		        , sj."ShipmentState"
		        from shipment s 
		        inner join shipment_head sh on sh.id=s."ShipmentHead_id"
		        inner join rela_data rd on rd."TableName1" ='suju' and rd."TableName2" ='shipment' and rd."DataPk2" =s.id
		        inner join suju sj on sj.id = rd."DataPk1" 
		        where sh.id = :sh_id
		        )
		        update suju set "ShipmentState" ='inpec'
		        from A where A.suju_id = id
				""";

		this.sqlRunner.execute(sql, paramMap);
	}

}
