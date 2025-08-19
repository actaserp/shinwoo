package mes.domain.repository;

import mes.domain.entity.JobRes;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.RoutingProc;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface RoutingProcRepository extends JpaRepository<RoutingProc, Integer>{

	Integer countByRoutingId(Integer routingPk);

	RoutingProc getRoutingProcById(Integer id);

	@Query("select rp from RoutingProc rp where rp.routingId = :routingId order by rp.processOrder asc")
	List<RoutingProc> findByRoutingIdOrderByProcessOrder(@Param("routingId") Integer routingId);

	@Modifying(clearAutomatically = true, flushAutomatically = true)
	@Query(value = "DELETE FROM routing_proc WHERE \"Routing_id\" = :routingId", nativeQuery = true)
	int deleteByRoutingId(@Param("routingId") Integer routingId);

	@Query("select coalesce(max(rp.processOrder), 0) from RoutingProc rp where rp.routingId = :routingId")
	Integer findMaxOrderByRoutingId(@Param("routingId") Integer routingId);

	@Modifying(clearAutomatically = true)
	@Query(value = """
      UPDATE routing_proc
      SET "ProcessOrder" = -"ProcessOrder"
      WHERE "Routing_id" = :routingId
    """, nativeQuery = true)
	int bumpAllByRoutingIdNegate(@Param("routingId") Integer routingId);

	// 선택된 한 건에 순번 부여 (배열 없이 개별 업데이트)
	@Modifying(clearAutomatically = true)
	@Query(value = """
      UPDATE routing_proc
      SET "ProcessOrder" = :ord
      WHERE id = :id
    """, nativeQuery = true)
	int assignOrder(@Param("id") Integer id, @Param("ord") Integer ord);

	// 나머지(아직 음수인 것들)를 뒤에 이어서 번호 부여
	@Modifying(clearAutomatically = true)
	@Query(value = """
      WITH rest AS (
        SELECT rp.id,
               ROW_NUMBER() OVER (ORDER BY rp."ProcessOrder") + :base AS ord
        FROM routing_proc rp
        WHERE rp."Routing_id" = :routingId
          AND rp."ProcessOrder" < 0
      )
      UPDATE routing_proc rp
      SET "ProcessOrder" = rest.ord
      FROM rest
      WHERE rp.id = rest.id
    """, nativeQuery = true)
	int reorderRestSimple(@Param("routingId") Integer routingId,
						  @Param("base") int base);
}
