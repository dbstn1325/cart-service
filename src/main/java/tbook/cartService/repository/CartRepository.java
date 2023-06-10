package tbook.cartService.repository;


import org.springframework.data.jpa.repository.JpaRepository;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;

import java.util.List;
import java.util.Optional;

public interface CartRepository extends JpaRepository<Cart, Long> {
//    boolean existsByProductName(String productName);
    Optional<List<Cart>> findByUserId(String userId);
}