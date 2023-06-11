package tbook.cartService.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CartResponse {
    private Long cartId;
    private Long productId;
    private String productName;
    private String productMadeBy;
    private String productImage;
    private int quantity;
    private int unitPrice;
    private int totalPrice;
}
