package tbook.cartService.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;

import java.util.List;

public interface CartRepository extends JpaRepository<Cart, Long> {
    boolean existsByProductName(String productName);
}