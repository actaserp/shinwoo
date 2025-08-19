package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Date;

@Entity
@Table(name="bom_proc_comp")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class BomProcComp extends AbstractAuditModel {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;

	@Column(name = "\"Amount\"")
	private Double amount;

	@Column(name = "\"BOM_id\"")
	private Integer bomId;

	@Column(name = "\"Material_id\"", nullable = false)
	private Integer materialId;   // 소비 자재

	@Column(name = "\"Process_id\"", nullable = false)
	private Integer processId;    // 공정

	@Column(name = "\"Product_id\"", nullable = false)
	private Integer productId;    // 해당 공정의 대상(산출) 품목

	@Column(name = "\"Routing_id\"")
	private Integer routingId;

	@Column(name = "spjangcd")
	private String spjangcd;

	@Column(name = "vercode")
	private String vercode;
}
