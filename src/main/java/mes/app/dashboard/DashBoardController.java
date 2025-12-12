package mes.app.dashboard;

import java.sql.Timestamp;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import lombok.extern.slf4j.Slf4j;
import mes.app.balju.service.BaljuOrderService;
import mes.app.sales.service.SujuService;
import mes.domain.entity.*;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import mes.app.dashboard.service.DashBoardService;
import mes.domain.model.AjaxResult;

@Slf4j
@RestController
@RequestMapping("/api/dashboard")
public class DashBoardController {

	
	@Autowired
	private DashBoardService dashBoardService;

	@Autowired
	BaljuOrderService baljuOrderService;

	@Autowired
	SujuService sujuService;

	@Autowired
	SujuHeadRepository sujuHeadRepository;

	@Autowired
	BalJuHeadRepository balJuHeadRepository;

	@Autowired
	TB_InvoicementRepository invoicementRepository;

	@Autowired
	TB_SalesmentRepository salesmentRepository;

	@Autowired
	TB_BANKTRANSITRepository banktransitRepository;

	@GetMapping("/read")
	public AjaxResult getSujuList(
			@RequestParam(value="date_from", required=false) String start_date,
			@RequestParam(value="date_to", required=false) String end_date,
			@RequestParam(value="choComp", required=false) String choComp,
			@RequestParam(value="spjangcd") String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.dashBoardService.getOverview(start, end, spjangcd, choComp);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	@GetMapping("/detail")
	public AjaxResult getDetail(
			@RequestParam("id") int id,
			@RequestParam("division") String division,
			@RequestParam(value = "company_id", required = false) Integer company_id,
			@RequestParam(value = "JumunDate", required = false) String JumunDate,
			@RequestParam(value = "startDate", required = false) String startDate,
			@RequestParam(value = "endDate", required = false) String endDate,
			Authentication auth,
			HttpServletRequest request) {
		String key = (division == null) ? "" : division.trim();
//		log.info("상세 - division:{}, id:{}", division, id);
		User user = (User) auth.getPrincipal();
		String spjangcd = user.getSpjangcd();
		List<Map<String, Object>> item = new ArrayList<>();
		switch (key) {
			case "발주":
//				item = dashBoardService.getBaljuDetail(id);
				Map<String, Object> purchaseDetail = new HashMap<>();
				// 출금 매입 그리드 조회
				purchaseDetail.put("TotalList", dashBoardService.getPayableTotalList(startDate, endDate, company_id, spjangcd));
				// 출금 매입 미수총액 등 조회
				purchaseDetail.put("getDetail", dashBoardService.getDetailFinanceTotal(company_id, spjangcd, JumunDate));
				item.add(purchaseDetail);
				break;
			case "수주":
				item = dashBoardService.getSujuDetail(id);
				break;
			case "매입":
				Map<String, Object> baljuDetail = new HashMap<>();
				// 출금 매입 그리드 조회
				baljuDetail.put("TotalList", dashBoardService.getPayableTotalList(startDate, endDate, company_id, spjangcd));
				// 출금 매입 미수총액 등 조회
				baljuDetail.put("getDetail", dashBoardService.getDetailFinanceTotal(company_id, spjangcd, JumunDate));
				item.add(baljuDetail);
				break;
			case "매출":
				Map<String, Object> salesDetail = new HashMap<>();
				// 입금 매출 그리드 조회
				salesDetail.put("TotalList", dashBoardService.getDepositTotalList(startDate, endDate, company_id, spjangcd));
				// 입금 매출 미수총액 등 조회
				salesDetail.put("getDetail", dashBoardService.getDetailFinanceTotal(company_id, spjangcd, JumunDate));
				item.add(salesDetail);
				break;
			case "입금":
				Map<String, Object> depDetail = new HashMap<>();
				// 입금 매출 그리드 조회
				depDetail.put("TotalList", dashBoardService.getDepositTotalList(startDate, endDate, company_id, spjangcd));
				// 입금 매출 미수총액 등 조회
				depDetail.put("getDetail", dashBoardService.getDetailFinanceTotal(company_id, spjangcd, JumunDate));
				item.add(depDetail);
				break;
			case "출금":
				Map<String, Object> wdrawDetail = new HashMap<>();
				// 출금 매입 그리드 조회
				wdrawDetail.put("TotalList", dashBoardService.getPayableTotalList(startDate, endDate, company_id, spjangcd));
				// 출금 매입 미수총액 등 조회
				wdrawDetail.put("getDetail", dashBoardService.getDetailFinanceTotal(company_id, spjangcd, JumunDate));
				item.add(wdrawDetail);
				break;

			default:
				AjaxResult err = new AjaxResult();
				err.success = false;
				err.message = "지원하지 않는 구분입니다: " + key;
				return err;
		}

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@GetMapping("/history")
	public AjaxResult getHistory(
			@RequestParam("id") int id,
			@RequestParam("division") String division,
			HttpServletRequest request) {

		String key = (division == null) ? "" : division.trim();

//		log.info("division:{}, id:{}", division, id);
		List<Map<String, Object>> item;
		switch (key) {
			case "발주":
				item = dashBoardService.getBaljuHistory(id);
				break;

			case "수주":
				item = dashBoardService.getSujuHistory(id);
				break;

			case "매입":
				item = dashBoardService.getInvoHistory(id);
				break;

			case "매출":
				item = dashBoardService.getSalesHistory(id);
				break;
			case "입금":
				item = dashBoardService.getReceiveHistory(id);
				break;
			case "출금":
				item = dashBoardService.getPaymentHistory(id);
				break;

			default:
				AjaxResult err = new AjaxResult();
				err.success = false;
				err.message = "지원하지 않는 구분입니다: " + key;
				return err;
		}

		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}


	@GetMapping("/company")
	public AjaxResult getCompany(
			@RequestParam("comp_id") int comp_id,
			HttpServletRequest request) {

		Map<String, Object> item = dashBoardService.getCompany(comp_id);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@PostMapping("/memo/save")
	@Transactional
	public AjaxResult memoSave(
			@RequestParam("head_id") int id,
			@RequestParam("division") String division,
			@RequestParam("description") String description) {

		switch (division) {
			case "발주": {
				BaljuHead e = balJuHeadRepository.findById(id)
						.orElseThrow(() -> new RuntimeException("발주 헤더 없음"));
				e.setDescription(description);
				balJuHeadRepository.save(e);
				break;
			}
			case "수주": {
				SujuHead e = sujuHeadRepository.findById(id)
						.orElseThrow(() -> new RuntimeException("수주 헤더 없음"));
				e.setDescription(description);
				sujuHeadRepository.save(e);
				break;
			}
			case "매입": {

				TB_Invoicement e = (TB_Invoicement) invoicementRepository.findByMisnum(id)
						.orElseThrow(() -> new RuntimeException("매입 전표 없음"));
				// 목록/이력에서 사용한 컬럼: s.remark1
				e.setRemark1(description);
				invoicementRepository.save(e);
				break;
			}
			case "매출": {
				TB_Salesment e = (TB_Salesment) salesmentRepository.findByMisnum(id)
						.orElseThrow(() -> new RuntimeException("매출 전표 없음"));
				e.setRemark1(description);
				salesmentRepository.save(e);
				break;
			}
			case "입금": {
				// tb_banktransit: 키는 ioid (입금/출금 공용)
				TB_BANKTRANSIT e = banktransitRepository.findById(id)
						.orElseThrow(() -> new RuntimeException("입금 내역 없음"));
				e.setMemo(description);
				banktransitRepository.save(e);
				break;
			}
			case "출금": {
				TB_BANKTRANSIT e = banktransitRepository.findById(id)
						.orElseThrow(() -> new RuntimeException("출금 내역 없음"));
				e.setMemo(description);
				banktransitRepository.save(e);
				break;
			}
			default:
				throw new IllegalArgumentException("지원하지 않는 구분: " + division);
		}

		AjaxResult result = new AjaxResult();
		result.success = true;
		result.message = "저장을 성공했습니다.";
		return result;
	}

	@GetMapping("/today_week_prod")
	public AjaxResult todayWeekProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		List<Map<String, Object>> items = dashBoardService.todayWeekProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/today_prod")
	public AjaxResult todayProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = dashBoardService.todayProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/year_def_prod")
	public AjaxResult yearDefProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = dashBoardService.yearDefProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/mat_stock")
	public AjaxResult matStock(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = dashBoardService.matStock(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_order")
	public AjaxResult customOrder(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = dashBoardService.customOrder(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat")
	public AjaxResult customServiceStat(
			@RequestParam(value="dateType", required=false) String dateType) 
	{
		Map<String, Object> items = dashBoardService.customServiceStat(dateType);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat_result")
	public AjaxResult customServiceStatResult()
	{
		List<Map<String, Object>> items = dashBoardService.customServiceStatResult();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/haccp_read")
	public AjaxResult haccp_read(
			@RequestParam("year_month") String year_month,
			@RequestParam("data_year") String data_year,
			@RequestParam("data_month") String data_month,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = dashBoardService.haccpReadResult(year_month,data_year,data_month,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
		
		
	@GetMapping("/getCppList")
	public AjaxResult getCppList(
			@RequestParam("strDate") String strDate,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = dashBoardService.getCppList(strDate,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/detail_haccp_process")
	public AjaxResult detail_haccp_process(
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> haccpProcessItems = dashBoardService.getDetailHacpPro();
		
		AjaxResult result = new AjaxResult();
		result.data = haccpProcessItems;
		
		return result;
	}


	@GetMapping("/job_state_read")
	public AjaxResult getJobStateRead(
		@RequestParam(value = "date_from", required = false) String dateFrom,
		@RequestParam(value = "date_to", required = false) String dateTo,
		@RequestParam(value = "is_include_comp", required = false) String isIncludeComp,
		@RequestParam(value="factory", required=false) Integer cboFactory,
		@RequestParam(value = "choMat", required = false) String choMat,
		@RequestParam(value = "company", required = false) String company,
		@RequestParam("spjangcd") String spjangcd) {

		List<Map<String, Object>> items = this.dashBoardService.getJobStateRead(dateFrom, dateTo, isIncludeComp, spjangcd, choMat, cboFactory, company);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	
	
}
