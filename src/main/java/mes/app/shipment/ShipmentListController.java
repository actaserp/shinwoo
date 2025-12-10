package mes.app.shipment;

import java.util.List;
import java.util.Map;

import mes.domain.entity.Shipment;
import mes.domain.entity.User;
import mes.domain.repository.ShipmentHeadRepository;
import mes.domain.repository.ShipmentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.web.bind.annotation.*;

import mes.app.shipment.service.ShipmentListService;
import mes.domain.model.AjaxResult;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/shipment/shipment_list")
public class ShipmentListController {
	
	@Autowired
	private ShipmentListService shipmentListService;

	@Autowired
	ShipmentRepository shipmentRepository;

	@Autowired
	ShipmentHeadRepository shipmentHeadRepository;
	
	@GetMapping("/shipment_head_list")
	public AjaxResult getShipmentHeadList(
			@RequestParam("srchStartDt") String dateFrom,
			@RequestParam("srchEndDt") String dateTo,
			@RequestParam(value = "cboCompany", required = false) String compPk,
			@RequestParam("cboMatGroup") String matGrpPk,
			@RequestParam("cboMaterial") String matPk,
			@RequestParam(value = "CompanySearch", required = false) String company,
			@RequestParam("keyword") String keyword
	) {
		
		List<Map<String, Object>> items = this.shipmentListService.getShipmentHeadList(dateFrom,dateTo,compPk,matGrpPk,matPk,keyword, "shipped", company);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/shipment_item_list")
	public AjaxResult getShipmentItemList(
			@RequestParam("head_id") String headId
			) {
		
		List<Map<String, Object>> items = this.shipmentListService.getShipmentItemList(headId, null);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@Transactional
	@PostMapping("/shipment_status_cancel")
	public AjaxResult shipmentStatusCancel(
			@RequestParam(value = "cancel_ids[]", required = false) List<Integer> cancelIds,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		if (cancelIds == null || cancelIds.isEmpty()) {
			result.success = false;
			result.message = "선택된 출고가 없습니다.";
			return result;
		}

		try {
			for (Integer shId : cancelIds) {

				// Shipment 리스트 조회
				List<Shipment> smList = this.shipmentRepository.findByShipmentHeadId(shId);
				if (smList == null || smList.isEmpty()) continue;

				for (Shipment sm : smList) {
					// 출고 확정("a") 상태인 건만 취소 처리
					if (sm != null && "a".equals(sm.get_status())) {
						sm.setQty(0.0);
						sm.set_status("t");        // 임시 상태로 되돌림
						sm.set_audit(user);
						this.shipmentRepository.save(sm);
					}
				}

				// 수주헤더 기준으로 출하항목(shipment) state 변경
				this.shipmentListService.updateShipmentStateCancel(shId);

				// 관련 수주 상태도 취소 처리
				this.shipmentListService.updateSujuShipmentStateCancel(shId);
			}

			result.success = true;
			result.message = "출고 취소 처리가 완료되었습니다.";

		} catch (Exception ex) {
			result.success = false;
			result.message = "출고 취소 중 오류 발생: " + ex.getMessage();
			ex.printStackTrace();
			TransactionAspectSupport.currentTransactionStatus().setRollbackOnly(); // 롤백
		}

		return result;
	}



}
