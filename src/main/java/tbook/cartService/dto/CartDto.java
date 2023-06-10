package tbook.cartService.dto;

import lombok.Builder;
import lombok.Data;

@Data
public class CartDto {
    private String userId;
    private String productId;
    private String productName;
    private String productMadeBy;
    private String productImage;
    private int quantity;
    private int unitPrice;


    @Builder
    public CartDto(String userId, String productId, String productName, String productMadeBy, String productImage, int quantity, int unitPrice) {
        this.userId = userId;
        this.productId = productId;
        this.productName = productName;
        this.productMadeBy = productMadeBy;
        this.productImage = productImage;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
    }
}
