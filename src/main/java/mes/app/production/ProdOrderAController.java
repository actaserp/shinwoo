package mes.app.production;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.domain.entity.*;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdOrderAService;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/production/prod_order_a")
public class ProdOrderAController {
	
	@Autowired
	private ProdOrderAService prodOrderAService;
	
	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired
	JobResRepository jobResRepository;
	
	@Autowired
	RoutingProcRepository routingProcRepository;

	@Autowired
	WorkcenterRepository workcenterRepository;

	@Autowired
	BomProcCompRepository bomProcCompRepository;

	@Autowired
	BomRepository bomRepository;
	
	@GetMapping("/read")
	public AjaxResult getProdOrderA(
			@RequestParam(value="date_from", required = true) String dateFrom,
			@RequestParam(value="date_to", required = true) String dateTo,
			@RequestParam("workcenter_pk") String workcenterPk,
			@RequestParam("mat_type") String matType,
			@RequestParam("mat_grp_pk") String matGrpPk,
			@RequestParam("keyword") String keyword,
			@RequestParam("spjangcd") String spjangcd
			){
		List<Map<String, Object>> items = this.prodOrderAService.getProdOrderA(dateFrom,dateTo,matGrpPk,keyword,matType,workcenterPk,spjangcd);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/mat_info")
	public AjaxResult getMatInfo(
			@RequestParam("id") String id ) {
		
		Map<String, Object> items = this.prodOrderAService.getMatInfo(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getProdOrderADetail(
			@RequestParam("jr_pk") String jrPk) {
		
		Map<String, Object> items = this.prodOrderAService.getProdOrderADetail(jrPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@Transactional
	@PostMapping("/save")
	public AjaxResult saveProdOrderA(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("production_date") String productionDate,
			@RequestParam(value = "cboEquipment", required=false) Integer cboEquipment,
			@RequestParam("cboMaterial") Integer cboMaterial,
			@RequestParam("cboMaterialGrp") Integer cboMaterialGrp,
			@RequestParam("cboShiftCode") String cboShiftCode,
			@RequestParam("cboWorcenter") Integer cboWorcenter, // 헤더 기본 워크센터(라우팅 없을 때만 사용)
			@RequestParam("txtDescription") String txtDescription,
			@RequestParam("txtOrderQty") Integer txtOrderQty,
			@RequestParam("txtUnit") String txtUnit,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User) auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		Integer matPk = cboMaterial;
		Material m = materialRepository.getMaterialById(matPk);
		Integer routingPk = m.getRoutingId();
		Integer locPk = m.getStoreHouseId();

		Timestamp prodDate = CommonUtil.tryTimestamp(productionDate);

		// 신규 or 수정 검증
		JobRes header;
		if (id != null) {
			header = jobResRepository.getJobResById(id);
			if (!"ordered".equals(header.getState())) {
				result.success = false;
				result.message = "지시중 상태에서만 수정 가능합니다.";
				return result;
			}
		} else {
			header = new JobRes();
		}

		final boolean hasRouting = (routingPk != null);

		// ===== 헤더 저장 =====
		header.set_audit(user);
		header.setProductionDate(prodDate);
		header.setProductionPlanDate(prodDate);
		header.setMaterialId(matPk);
		header.setOrderQty((float) txtOrderQty);
		header.setDescription(txtDescription);
		header.setStoreHouse_id(locPk);
		header.setLotCount(1);
		header.setState("ordered");
		header.setSpjangcd(spjangcd);

		if (!hasRouting) {
			// 라우팅 없음 → 화면값 사용
			header.setRouting_id(null);
			header.setProcessCount(1);
			header.setWorkIndex(0); // 헤더만 존재
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
		header.setWorkIndex(last.getProcessOrder()); // 헤더=마지막 공정 순서
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

			child.setDescription(txtDescription);
			child.setParent_id(header.getId());
			child.setRouting_id(routingPk);
			child.setProcessCount(1);
			child.setWorkIndex(step.getProcessOrder());
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



	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteProdOrderA(@RequestParam("id") Integer id) {
		AjaxResult result = new AjaxResult();
		
		Map<String, Object> row = this.prodOrderAService.getJopResRow(id);
		
		if (row == null) {
			result.success = true;
			result.code = id.toString();
			return result;
		}
		int deletYn = this.prodOrderAService.deleteById(id);

		if (deletYn == -1) {
			result.success = false;
			result.message = "공정은 삭제할 수 없습니다.";
			return result;
		}
		if (deletYn == -2) {
			result.success = false;
			result.message = "공정 중 진행중 건이 있어 삭제할 수 없습니다.";
			return result;
		}
		if (deletYn <= 0) {
			result.success = false;
			result.message = "삭제할 작업지시가 없습니다.";
			return result;
		}


		Integer sujuPk = 0;
		if (row.get("state").equals("ordered")) {
			if (row.get("src_table") != null) {
				if (row.get("src_table").equals("suju")) {
					sujuPk = Integer.parseInt(row.get("src_pk").toString());
				}
			} else {
				sujuPk = 0;
			}
		}

		
		if (sujuPk > 0 && deletYn > 0) {
			this.prodOrderAService.updateBySujuPk(sujuPk);
		}
		
		return result;
	}
}
