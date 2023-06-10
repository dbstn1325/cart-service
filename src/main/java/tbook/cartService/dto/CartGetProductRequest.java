package tbook.cartService.dto;

import lombok.Data;

@Data
public class CartGetProductRequest {
    private String productId;
    private String userId;
}
