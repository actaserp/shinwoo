package mes.app.balju;

import lombok.extern.slf4j.Slf4j;
import mes.app.balju.service.BalJuOptimalStockService;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/balju/optimal_stock")
public class BalJuOptimalStockController {

  @Autowired
  BalJuOptimalStockService optimalStockService;

  @GetMapping("/read")
  public AjaxResult getList(@RequestParam(value = "mat_name", required = false) String mat_name,
                            @RequestParam(value = "Inventory_status", required = false) String status,
                            @RequestParam(value="srchStartDt") String  startDt,
                            @RequestParam(value="srchEndDt") String endDt,
                            @RequestParam(value = "spjangcd")String spjangcd) {
    AjaxResult result = new AjaxResult();
    /*log.info("자재 적정재고 현황 mat_name:{}, Inventory_status:{}, srchStartDt:{}, srchEndDt:{}, spjangcd:{}"
        , mat_name, status, startDt,endDt, spjangcd );*/

    startDt = startDt + " 00:00:00";
    endDt = endDt + " 23:59:59";

    Timestamp start = Timestamp.valueOf(startDt);
    Timestamp end = Timestamp.valueOf(endDt);

    List<Map<String,Object>> items = optimalStockService.getList(mat_name,status ,start,end, spjangcd);
    result.data = items;
    return result;
  }

}
