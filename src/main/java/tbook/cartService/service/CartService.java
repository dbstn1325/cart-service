package tbook.cartService.service;

import tbook.cartService.dto.CartRequest;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;

import java.util.List;

public interface CartService {
    List<CartResponse> getAllCarts();

    CartResponse addToCart(CartRequest cartRequest);

    void deleteCart(Long cartId);

    Cart incrementQuantity(Long cartId);

    Cart decreaseQuantity(Long cartId);

    int calculateTotalPrice();
}
