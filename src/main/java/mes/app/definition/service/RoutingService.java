package mes.app.definition.service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.repository.ProcessRepository;
import mes.domain.repository.RoutingRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.MultiValueMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class RoutingService {

    @Autowired
    RoutingRepository routingRepository;

    @Autowired
    SqlRunner sqlRunner;


    //공정 목록 조회
    public List<Map<String, Object>> getRoutingList(String routingName, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("routing_name", routingName);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                select r.id
                         , r."Name" as routing_name
                         , to_char(r."_created" ,'yyyy-mm-dd hh24:mi') as created
                         , s."Name" as "storeHouse_nm"
                         , r."StoreHouse_id" as "storeHouse_id"
                         , r."Description" as description
                         from routing r
                         left join store_house s on r."StoreHouse_id"=s.id
                         where 1=1
                         AND r.spjangcd = :spjangcd
                         """;
        if (!StringUtils.isEmpty(routingName))
            sql += "and upper(r.\"Name\") like concat('%%',upper(:routing_name),'%%')";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
    }

    public List<Map<String, Object>> getProcessList(int routingId) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("routingId", routingId);

        String sql = """
                select
                    rp.id
                    , rp."ProcessOrder"
                    , rp."Process_id"
                    , p."Code" as "ProcessCode"
                    , p."Name" as "ProcessName"
                    , to_char(rp._created ,'yyyy-mm-dd hh24:mi:ss') as _created\s
                from routing_proc rp
                left join process p on p.id = rp."Process_id"
                where "Routing_id" = :routingId
                order by rp."ProcessOrder"
                  """;

        List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);

        return item;
    }

}
