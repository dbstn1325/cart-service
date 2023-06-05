package tbook.cartService.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CartRequest implements Serializable {
    private String productName;
    private int quantity;
    private int unitPrice;
}
