package mes.app.dashboard;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import mes.app.balju.service.BaljuOrderService;
import mes.app.sales.service.SujuService;
import mes.domain.entity.BaljuHead;
import mes.domain.entity.SujuHead;
import mes.domain.repository.BalJuHeadRepository;
import mes.domain.repository.SujuHeadRepository;
import mes.domain.repository.SujuRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import mes.app.dashboard.service.DashBoardService;
import mes.domain.model.AjaxResult;

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
			HttpServletRequest request) {

		List<Map<String, Object>> item = null;
		if ("발주".equals(division)) {
			item = dashBoardService.getBaljuDetail(id);
		} else{
			item = dashBoardService.getSujuDetail(id);
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

		List<Map<String, Object>> item = null;
		if ("발주".equals(division)) {
			item = dashBoardService.getBaljuHistory(id);
		} else{
			item = dashBoardService.getSujuHistory(id);
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
	public AjaxResult memoSave(
			@RequestParam("head_id") int id,
			@RequestParam("division") String division,
			@RequestParam("description") String description,
			HttpServletRequest request) {

		AjaxResult result = new AjaxResult();
		if ("발주".equals(division)) {
			BaljuHead baljuHead = balJuHeadRepository.findById(id).orElseThrow(() -> new RuntimeException("발주 헤더 없음"));;
			baljuHead.setDescription(description);
			balJuHeadRepository.save(baljuHead);
		} else{
			SujuHead sujuHead = sujuHeadRepository.findById(id).orElseThrow(() -> new RuntimeException("수주 헤더 없음"));;
			sujuHead.setDescription(description);
			sujuHeadRepository.save(sujuHead);
		}

		result.success = true;
		result.message = "저장을 성공했습니다.";

		return result;
	}

	@GetMapping("/today_week_prod")
	private AjaxResult todayWeekProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.todayWeekProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/today_prod")
	private AjaxResult todayProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.todayProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/year_def_prod")
	private AjaxResult yearDefProd(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.yearDefProd(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/mat_stock")
	private AjaxResult matStock(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.matStock(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_order")
	private AjaxResult customOrder(
			@RequestParam("spjangcd") String spjangcd
	) {
		
		List<Map<String, Object>> items = this.dashBoardService.customOrder(spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat")
	private AjaxResult customServiceStat(
			@RequestParam(value="dateType", required=false) String dateType) 
	{
		Map<String, Object> items = this.dashBoardService.customServiceStat(dateType);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/custom_service_stat_result")
	private AjaxResult customServiceStatResult() 
	{
		List<Map<String, Object>> items = this.dashBoardService.customServiceStatResult();
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/haccp_read")
	private AjaxResult haccp_read(
			@RequestParam("year_month") String year_month,
			@RequestParam("data_year") String data_year,
			@RequestParam("data_month") String data_month,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = this.dashBoardService.haccpReadResult(year_month,data_year,data_month,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
		
		
	@GetMapping("/getCppList")
	private AjaxResult getCppList(
			@RequestParam("strDate") String strDate,
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> items = this.dashBoardService.getCppList(strDate,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	
	@GetMapping("/detail_haccp_process")
	private AjaxResult detail_haccp_process(
			Authentication auth,
			HttpServletRequest request
			) {
		
		Map<String, Object> haccpProcessItems = this.dashBoardService.getDetailHacpPro();
		
		AjaxResult result = new AjaxResult();
		result.data = haccpProcessItems;
		
		return result;
	}
	
	
	
	
}
