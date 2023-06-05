package tbook.cartService.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CartResponse {
    private String productName;
    private int quantity;
    private int unitPrice;
    private int totalPrice;
}
