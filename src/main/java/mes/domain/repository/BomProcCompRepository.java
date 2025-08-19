package mes.domain.repository;

import mes.domain.entity.BomProcComp;
import mes.domain.entity.JobRes;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BomProcCompRepository extends JpaRepository<BomProcComp, Integer> {
	@Query(value = """
        SELECT DISTINCT "Product_id"
        FROM bom_proc_comp
        WHERE "Routing_id" = :routingId
          AND "Process_id" = :processId
        """, nativeQuery = true)
	List<Integer> findDistinctProductIdsByRoutingAndProcess(@Param("routingId") Integer routingId,
															@Param("processId") Integer processId);
}

