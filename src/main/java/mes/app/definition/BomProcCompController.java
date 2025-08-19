package mes.app.definition;

import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.BomProcCompService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/definition/bom_proc_comp")
public class BomProcCompController {  //공정별 bom

  @Autowired
  BomProcCompService bomProcCompService;

  @GetMapping("/read")
  public AjaxResult getRoutingList(@RequestParam(value = "routing_name" ,required = false) String routing_name,
                                   @RequestParam(value = "spjangcd") String spjangcd){

    List<Map<String,Object>> items = bomProcCompService.getRoutingList(routing_name, spjangcd);

    AjaxResult result = new AjaxResult();
    result.data = items;
    return result;
  }

}
