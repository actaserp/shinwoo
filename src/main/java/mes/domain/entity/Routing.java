package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Table(name="routing")
@Setter
@Getter
@NoArgsConstructor
public class Routing extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Name\"")
	String name;

	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"StoreHouse_id\"")
	Integer storeHouse_id;

	String spjangcd;

}
