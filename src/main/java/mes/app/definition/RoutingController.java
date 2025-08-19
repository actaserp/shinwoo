package mes.app.definition;

import mes.app.definition.service.ProcessService;
import mes.app.definition.service.RoutingService;
import mes.domain.entity.*;
import mes.domain.entity.Process;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ProcessRepository;
import mes.domain.repository.RoutingProcRepository;
import mes.domain.repository.RoutingRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/definition/routing")
public class RoutingController {

	@Autowired
	RoutingRepository routingRepository;

	@Autowired
	RoutingProcRepository routingProcRepository;
	
	@Autowired
	private RoutingService routingService;
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 라우팅 목록 조회
	@GetMapping("/read")
	public AjaxResult getRoutingList(
			@RequestParam("keyword") String routingName,
			@RequestParam(value ="spjangcd") String spjangcd,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.routingService.getRoutingList(routingName,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 라우팅 정보 저장
	@PostMapping("/save")
	public AjaxResult saveRouting(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="routing_name") String name,
			@RequestParam(value="storeHouse_id", required=false) Integer storeHouse_id,
			@RequestParam(value="description", required=false) String description,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		Routing routing =null;

		boolean name_chk = this.routingRepository.findByName(name).isEmpty();


		// 이름 중복 체크
		if (id==null) {
			if (name_chk == false) {
				result.success = false;
				result.message="중복된 이름이 존재합니다.";
				return result;
			}
			routing = new Routing();
		} else {
			routing = this.routingRepository.getRoutingById(id);
			if (name.equals(routing.getName())==false && name_chk == false) {
				result.success = false;
				result.message="중복된 이름이 존재합니다.";
				return result;
			}
		}

		routing.setName(name);
		routing.setDescription(description);
		routing.setStoreHouse_id(storeHouse_id);
		routing.set_audit(user);
		routing.setSpjangcd(spjangcd);


		routing = this.routingRepository.save(routing);
	
        result.data=routing;
		
		return result;
	}
	
	// 라우팅 삭제
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deleteRouting(@RequestParam("id") int id) {

		this.routingProcRepository.deleteByRoutingId(id);
        this.routingRepository.deleteById(id);
        AjaxResult result = new AjaxResult();
        
		return result;
	}

	// 공정 추가
	@PostMapping("/add_process")
	public AjaxResult addProcess(
			@RequestParam("routing_id") Integer routing_id,
			@RequestParam("process_id") Integer process_id,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		Integer max = routingProcRepository.findMaxOrderByRoutingId(routing_id);
		int next = (max == null ? 0 : max) + 1;

		RoutingProc rp = new RoutingProc();

		rp.setRoutingId(routing_id);
		rp.setProcessOrder(next);
		rp.setProcessId(process_id);
		rp.set_audit(user);

		rp = this.routingProcRepository.save(rp);

		result.data=rp;

		return result;
	}
	
	// 순서 저장
	@Transactional
	@PostMapping("/save_index")
	public AjaxResult saveWorkIndex(@RequestBody MultiValueMap<String,Object> dataList,
									HttpServletRequest request,
									Authentication auth) {
		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();

		List<Map<String, Object>> items =
				CommonUtil.loadJsonListMap(dataList.getFirst("Q").toString());
		if (items.isEmpty()) { result.success = false; return result; }

		List<Integer> ids = items.stream()
				.map(m -> Integer.parseInt(m.get("id").toString()))
				.collect(Collectors.toList());

		RoutingProc any = routingProcRepository.getRoutingProcById(ids.get(0));
		Integer routingId = any.getRoutingId();

		// 1) 전부 음수로 (유니크 충돌 방지)
		routingProcRepository.bumpAllByRoutingIdNegate(routingId);

		// 2) 선택된 것들만 1..N으로
		int ord = 1;
		for (Integer id : ids) {
			routingProcRepository.assignOrder(id, ord++);
		}

		// 3) 나머지(아직 음수)들을 N+1.. 로
		routingProcRepository.reorderRestSimple(routingId, ids.size());

		result.success = true;
		return result;
	}

	
	// 공정정보 조회
	@GetMapping("/process_list")
	public AjaxResult getProcessList(
			@RequestParam("id") int routing_id,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.routingService.getProcessList(routing_id);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 라우팅에서 공정정보 삭제
	@PostMapping("/delete_process")
	public AjaxResult deleteProcess(
			@RequestParam("id") int id,
			@RequestParam("routing_id") int routing_id,
			@RequestParam("process_id") int process_id
	) {

		this.routingProcRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}



}
