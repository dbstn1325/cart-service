package tbook.cartService.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class CartDto implements Serializable {

    private String userId;

    private String productId;
    private String productName;
    private int quantity;
    private int unitPrice;
    private int totalPrice;

    private String cartId;

//    public void incrementQuantity() {
//        this.quantity += 1;
//    }
//
//    public void decrementQuantity() {
//        if(this.quantity > 0) {
//            this.quantity -= 1;
//        }
//    }
}
