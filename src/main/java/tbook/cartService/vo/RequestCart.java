package tbook.cartService.vo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class RequestCart {
    private String productId;
    private String productName;
    private int quantity;
    private int unitPrice;
}
