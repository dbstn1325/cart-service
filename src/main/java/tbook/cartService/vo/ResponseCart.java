package tbook.cartService.vo;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResponseCart {

    private String productId;
    private String productName;
    private int quantity;
    private int unitPrice;
    private int totalPrice;
}
