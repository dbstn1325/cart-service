package tbook.cartService.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.ColumnDefault;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;

@Entity
@Getter @Setter
@Table(name = "carts")
//@EntityListeners(AuditingEntityListener.class)
public class Cart{

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String userId;

    @Column(nullable = false)
    private String productId;

    @Column(nullable = false)
    private String productName;

    @Column(nullable = false)
    private String productMadeBy;

    @Column(nullable = false)
    private String productImage;

    @ColumnDefault("1")
    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private int unitPrice;

//    @Column(nullable = true)
//    private int totalPrice;

//    @CreatedDate
//    @Column(name = "created_at", nullable = false, updatable = false)
//    private LocalDateTime createdAt;

//    @Builder
//    public Cart(Long id, String userId, String productId, String productName, String productMadeBy, String productImage, int quantity, int unitPrice, LocalDateTime createdAt) {
//        this.id = id;
//        this.userId = userId;
//        this.productId = productId;
//        this.productName = productName;
//        this.productMadeBy = productMadeBy;
//        this.productImage = productImage;
//        this.quantity = quantity;
//        this.unitPrice = unitPrice;
//        this.createdAt = createdAt;
//    }
}
