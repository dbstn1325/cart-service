package tbook.cartService.service;

import tbook.cartService.dto.CartGetProductRequest;
import tbook.cartService.dto.CartRequest;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;

import java.util.List;

public interface CartService {
    List<CartResponse> getAllCartsByUserId(String userId);

    void createCart(CartGetProductRequest cartGetProductRequest);

    void deleteCart(Long cartId);

    Cart incrementQuantity(Long cartId);

    Cart decreaseQuantity(Long cartId);

    int calculateTotalPrice();
}
