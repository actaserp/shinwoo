package mes.domain.repository;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.Bom;
import org.springframework.data.jpa.repository.Query;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.List;

public interface BomRepository extends JpaRepository<Bom, Integer>{
	public Bom getBomById(int id);

    List<Bom> findAllByStartDate(Timestamp startDate);

    Bom findByMaterialIdAndBomTypeAndVersion(Integer productId, String manufacturing, String s);

    // 지붕(최종품) → 해당 공정 대상(stepProductId)이 레벨-1로 몇 개 필요한지
    @Query(value = """
  SELECT COALESCE(SUM(bc."Amount"/NULLIF(b."OutputAmount",0)), 0)
  FROM bom b
  JOIN bom_comp bc ON bc."BOM_id" = b.id
  WHERE b."Material_id" = :finalProduct   -- 예: 지붕
    AND bc."Material_id" = :stepProduct   -- 예: 판넬
""", nativeQuery = true)
    BigDecimal findLevel1Factor(@Param("finalProduct") Integer finalProduct,
                                @Param("stepProduct") Integer stepProduct);
}
