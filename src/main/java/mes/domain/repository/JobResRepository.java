package mes.domain.repository;

import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import mes.domain.entity.JobRes;

@Repository
public interface JobResRepository extends JpaRepository<JobRes, Integer> {
	
	JobRes getJobResById(Integer id);

	List<JobRes> getJobResByParentId(Integer parentId);

	List<JobRes> findBySourceDataPkAndSourceTableName(Integer id, String string);

	List<JobRes> findBySourceDataPkAndSourceTableNameAndMaterialIdAndIdNotIn(Integer id, String string,
			Integer material_id, List<Integer> id2);

	@Modifying
	@Query("UPDATE JobRes j SET j.state = :state WHERE j.id = :jrPk")
	void updateStateById(@Param("jrPk") Integer jrPk, @Param("state") String state);

	@Query("""
		SELECT jr.id
		FROM JobRes jr
		JOIN Workcenter wc ON wc.id = jr.workCenter_id
		WHERE jr.workOrderNumber = :orderNum
		  AND wc.processId = :processId
		  AND jr.materialId = :prodMatId
		  AND jr.state <> 'canceled'
		ORDER BY jr.id DESC
	""")
	Integer findIdByOrderProcessAndMaterial(String orderNum, Integer processId, Integer prodMatId);

	// 공정 순서
	@Query("""
		SELECT rp.processOrder
		FROM RoutingProc rp
		WHERE rp.routingId = :routingId AND rp.processId = :processId
	""")
	Integer findProcessOrder(Integer routingId, Integer processId);
}
