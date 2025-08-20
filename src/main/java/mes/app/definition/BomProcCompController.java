package mes.app.definition;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.BomProcCompService;
import mes.domain.entity.BomProcComp;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BomProcCompRepository;
import mes.domain.services.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/definition/bom_proc_comp")
public class BomProcCompController {  //공정별 bom

  @Autowired
  BomProcCompService bomProcCompService;

  @Autowired
  BomProcCompRepository bomProcCompRepository;

  @GetMapping("/read")
  public AjaxResult getBomProcCompList(@RequestParam(value = "routing_name", required = false) String routing_name,
                                       @RequestParam(value = "spjangcd") String spjangcd) {

    List<Map<String, Object>> items = bomProcCompService.getBomProcCompList(routing_name, spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;
    return result;
  }


  @PostMapping("/save")
  @Transactional
  public AjaxResult saveBomProc(@RequestBody Map<String, List<Map<String, Object>>> request, Authentication auth) {
    List<Map<String, Object>> list = request.get("list");
    User user = (User) auth.getPrincipal();

    log.info(" 공정별bom 저장 list:{}", list);
    AjaxResult result = new AjaxResult();

    if (list == null || list.isEmpty()) {
      result.success = false;
      result.message = "저장할 데이터가 없습니다.";
      return result;
    }

    String spjangcd = user.getSpjangcd();

    try {
      for (Map<String, Object> row : list) {

        Integer id = toInt(row.get("id"));  // OK
        Integer bomId = toInt(row.get("bom_id"));
        Integer routingId = toInt(row.get("routing_id"));
        Integer processId = toInt(row.get("process_id"));
        Integer materialId = toInt(row.get("material_id"));
        Integer productId = toInt(row.get("product_id"));
        Double amount = toDouble(row.get("amount"));

       /* if (bomId == null || processId == null || materialId == null || productId == null || amount == null)
          throw new IllegalArgumentException("필수값이 누락되었습니다.");*/

        BomProcComp entity;

        if (id != null) {
          // 수정
          entity = bomProcCompRepository.findById(id).orElse(new BomProcComp());
          entity.setId(id); // 혹시 생성된 새 객체면 id 지정
        } else {
          // 신규
          entity = new BomProcComp();
        }

        entity.setBomId(bomId);
        entity.setRoutingId(routingId);
        entity.setProcessId(processId);
        entity.setMaterialId(materialId);
        entity.setProductId(productId);
        entity.setAmount(amount);
        entity.setSpjangcd(spjangcd);
        entity.set_audit(user);

        log.info("저장 대상 entity: {}", new ObjectMapper().writeValueAsString(entity));

        bomProcCompRepository.save(entity);  // id가 있으면 update, 없으면 insert
      }

      result.success = true;
      result.message = "저장 완료";
      return result;

    } catch (Exception e) {
      log.error("공정별 BOM 저장 중 예외 발생", e);
      result.success = false;
      result.message = "저장 중 오류 발생: " + e.getMessage();
      return result;
    }
  }

  private Integer toInt(Object val) {
    if (val == null) return null;
    if (val instanceof Integer) return (Integer) val;
    if (val instanceof Number) return ((Number) val).intValue();
    if (val instanceof String && !((String) val).trim().isEmpty()) {
      return Integer.parseInt((String) val);
    }
    return null;
  }

  private Double toDouble(Object val) {
    if (val == null) return null;
    if (val instanceof Double) return (Double) val;
    if (val instanceof Number) return ((Number) val).doubleValue();
    if (val instanceof String && !((String) val).trim().isEmpty()) {
      return Double.parseDouble((String) val);
    }
    return null;
  }


}
