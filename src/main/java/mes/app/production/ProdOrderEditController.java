package mes.app.production;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.domain.entity.*;
import mes.domain.repository.*;
import mes.domain.services.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdOrderEditService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/production/prod_order_edit")
public class ProdOrderEditController {

	@Autowired
	private ProdOrderEditService prodOrderEditService;

	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired
	RoutingProcRepository routingProcRepository;
	
	@Autowired
	JobResRepository jobResRepository;
	
	@Autowired
	SujuRepository sujuRepository;

	@Autowired
	WorkcenterRepository workcenterRepository;

	@Autowired
	BomProcCompRepository bomProcCompRepository;

	@Autowired
	BomRepository bomRepository;
	
	// 수주 목록 조회
	@GetMapping("/suju_list")
	public AjaxResult getSujuList(
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="start", required=false) String start,
			@RequestParam(value="end", required=false) String end,
			@RequestParam(value="mat_group", required=false) Integer mat_group,
			@RequestParam(value="mat_name", required=false) String mat_name,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="not_flag", required=false) String not_flag) {

		List<Map<String, Object>> items = this.prodOrderEditService.getSujuList(date_kind, start, end, mat_group, mat_name, not_flag, spjangcd);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}

	// 제품 지시내역 조회
	@GetMapping("/joborder_list")
	public AjaxResult getJobOrderList(
			@RequestParam(value="suju_id", required=false) Integer suju_id) {

		List<Map<String, Object>> items = this.prodOrderEditService.getJobOrderList(suju_id);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}

	// 제품 지시내역 상세조회
	@GetMapping("/joborder_detail")
	public AjaxResult getJobOrderDetail(
			@RequestParam("jobres_id") Integer jobres_id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.prodOrderEditService.getJobOrderDetail(jobres_id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 반제품 작업지시 조회
	@GetMapping("/semi_list")
	public AjaxResult getSemiList(
			@RequestParam(value="data_date", required=false) String data_date,
			@RequestParam(value="mat_pk", required=false) Integer mat_pk,
			@RequestParam(value="suju_qty", required=false) Integer suju_qty,
			@RequestParam(value="suju_pk", required=false) Integer suju_pk) {
		
		List<Map<String, Object>> items = this.prodOrderEditService.getSemiList(data_date, mat_pk, suju_qty, suju_pk);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}
		
	// 반제품 지시내역 조회
	@GetMapping("/semi_joborder_list")
	public AjaxResult getSemiJoborderList(
			@RequestParam(value="suju_id", required=false) Integer suju_id) {

		List<Map<String, Object>> items = this.prodOrderEditService.getSemiJoborderList(suju_id);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		
        return result;
	}
	
	// 작업지시 생성
	@PostMapping("/make_prod_order")
	@Transactional
	public AjaxResult makeProdOrder(
			@RequestParam(value="suju_id", required=false) Integer sujuId,
			@RequestParam(value="prod_date", required=false) String productionDate,
			@RequestParam(value="Material_id", required=false) Integer cboMaterial,
			@RequestParam(value="workshift", required=false) String cboShiftCode,
			@RequestParam(value="workcenter_id", required=false) Integer cboWorcenter,
			@RequestParam(value="equ_id", required=false) Integer cboEquipment,
			@RequestParam(value="AdditionalQty", required=false) Float txtOrderQty,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();

		Integer matPk = cboMaterial;
		Material m = materialRepository.getMaterialById(matPk);
		Integer routingPk = m.getRoutingId();
		Integer locPk = m.getStoreHouseId();

		Timestamp prodDate = CommonUtil.tryTimestamp(productionDate);

		// 신규 or 수정 검증
		JobRes header = new JobRes();

		final boolean hasRouting = (routingPk != null);

		// ===== 헤더 저장 =====
		header.set_audit(user);
		header.setProductionDate(prodDate);
		header.setProductionPlanDate(prodDate);
		header.setMaterialId(matPk);
		header.setOrderQty((float) txtOrderQty);
		header.setStoreHouse_id(locPk);
		header.setLotCount(1);
		header.setState("ordered");
		header.setSourceDataPk(sujuId);
		header.setSourceTableName("suju");
		header.setSpjangcd(spjangcd);

		if (!hasRouting) {
			// 라우팅 없음 → 화면값 사용
			header.setRouting_id(null);
			header.setProcessCount(1);
			header.setWorkCenter_id(cboWorcenter);
			header.setFirstWorkCenter_id(cboWorcenter);
			header.setEquipment_id(cboEquipment);
			header.setShiftCode(cboShiftCode);
			header = jobResRepository.save(header); // 트리거가 번호 생성
			result.success = true;
			result.data = header;
			return result;
		}

		// 라우팅 있음 → 공정 목록
		List<RoutingProc> steps = routingProcRepository.findByRoutingIdOrderByProcessOrder(routingPk);
		if (steps == null || steps.isEmpty()) {
			result.success = false;
			result.message = "라우팅 공정이 없습니다.";
			return result;
		}

		// 마지막 공정 = 헤더
		RoutingProc last = steps.get(steps.size() - 1);
		Integer lastProcId = last.getProcessId();
		Workcenter lastWc = workcenterRepository.findByProcessId(lastProcId);
		Integer lastWcId = (lastWc != null ? lastWc.getId() : null);

		header.setRouting_id(routingPk);
		header.setProcessCount(steps.size()); // 전체 공정 수
		header.setWorkCenter_id(lastWcId);
		header.setFirstWorkCenter_id(lastWcId);
		header.setEquipment_id(cboEquipment);   // 설비/교대는 라우팅 있을 땐 화면값 미사용
		header.setShiftCode(cboShiftCode);

		header = jobResRepository.save(header); // 트리거가 헤더 번호 생성

		// ===== 자식(전 공정들) 생성: 마지막 공정 제외 =====
		for (int i = 0; i < steps.size() - 1; i++) {
			RoutingProc step = steps.get(i);
			Integer processId = step.getProcessId();

			// 공정→워크센터
			Workcenter wc = workcenterRepository.findByProcessId(processId);
			Integer wcId = (wc != null ? wc.getId() : null);

			// ★ 공정 대상(Product) 조회
			List<Integer> prodIds = bomProcCompRepository
					.findDistinctProductIdsByRoutingAndProcess(routingPk, processId);

			Integer stepProductId = null;
			if (prodIds != null && !prodIds.isEmpty()) {
				// 다수면 헤더 품목과 일치하는 게 있으면 우선, 없으면 첫 번째
				stepProductId = prodIds.contains(matPk) ? matPk : prodIds.get(0);
			}
			if (stepProductId == null) stepProductId = matPk; // fallback

			JobRes child = new JobRes();
			child.set_audit(user);

			child.setProductionDate(prodDate);
			child.setProductionPlanDate(prodDate);

			// 자식 공정의 대상 품목으로 설정
			child.setMaterialId(stepProductId);

			// 자식 지시량을 넣게되면 생산해야할것 같아서 일단 뺌
			// 자식 수량을 공정 대상에 맞춰 스케일링하려면 아래 참고 섹션 참조
//			BigDecimal factor = bomRepository.findLevel1Factor(matPk, stepProductId); // 지붕→판넬 2
//			float childQty = factor != null
//					? factor.multiply(BigDecimal.valueOf(txtOrderQty)).floatValue()
//					: (float) txtOrderQty;
//			child.setOrderQty(childQty);
			child.setOrderQty(null);

			child.setParent_id(header.getId());
			child.setRouting_id(routingPk);
			child.setProcessCount(1);
			child.setWorkCenter_id(wcId);
			child.setFirstWorkCenter_id(wcId);
			child.setEquipment_id(null);
			child.setShiftCode(cboShiftCode);
			child.setStoreHouse_id(locPk);
			child.setState("ordered");
			child.setSpjangcd(spjangcd);

			jobResRepository.save(child);
		}


		result.success = true;
		result.data = header;
		return result;
	}

	// 지시내역 수정
	@PostMapping("/update_order")
	@Transactional
	public AjaxResult updateOrder(
			@RequestParam(value="id", required=false) Integer jobres_id,
			@RequestParam(value="ProductionDate", required=false) String productionDate,
			@RequestParam(value="ShiftCode", required=false) String ShiftCode,
			@RequestParam(value="WorkCenter_id", required=false) Integer WorkCenter_id,
			@RequestParam(value="Equipment_id", required=false) Integer Equipment_id,
			@RequestParam(value="OrderQty", required=false) Float OrderQty,
			@RequestParam(value="Description", required=false) String Description,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		Timestamp ProductionDate = Timestamp.valueOf(productionDate + " 00:00:00");
		
		JobRes jr = this.jobResRepository.getJobResById(jobres_id);
		
		if (jr != null) {
			
			jr.setProductionDate(ProductionDate);
			jr.setShiftCode(ShiftCode);
			jr.setWorkCenter_id(WorkCenter_id);
			jr.setOrderQty(OrderQty);
			jr.setDescription(Description);
			if (Equipment_id != null) {
				jr.setEquipment_id(Equipment_id);
			}
			jr.set_audit(user);

			jr = this.jobResRepository.save(jr);
						
			result.success = true;
		} else {
			result.success = false;					
		}
		
		return result;
	}
	
}
