package tbook.cartService.dto;

import lombok.Builder;
import lombok.Data;

import java.util.Map;

@Data
public class CartDto {
    private String userId;
    private String productId;
    private String productName;
    private String productMadeBy;
    private String productImage;
    private int quantity;
    private int unitPrice;

    public static CartDto fromMap(Map<Object, Object> productData) {
        return CartDto.builder()
                .userId(String.valueOf(productData.get("userId")))
                .productId(String.valueOf(productData.get("productId")))
                .productName(String.valueOf(productData.get("productName")))
                .productMadeBy(String.valueOf(productData.get("productMadeBy")))
                .productImage(String.valueOf(productData.get("productImage")))
                .quantity(1)
                .unitPrice(Integer.parseInt(String.valueOf(productData.get("productPrice"))))
                .build();
    }

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
