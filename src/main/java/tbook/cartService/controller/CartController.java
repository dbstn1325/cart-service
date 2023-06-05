package tbook.cartService.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tbook.cartService.dto.CartRequest;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;
import tbook.cartService.repository.CartRepository;
import tbook.cartService.service.CartService;

import java.util.List;

@RestController
@RequestMapping("/cart-service")
@RequiredArgsConstructor
@Slf4j
public class CartController {

    private final CartService cartService;
    private final CartRepository cartRepository;

    // 장바구니 전체 조회
    @GetMapping("/carts")
    public ResponseEntity<List<CartResponse>> getAllCarts() {
        List<CartResponse> carts = cartService.getAllCarts();
        return ResponseEntity
                .status(HttpStatus.OK)
                .body(carts);
    }

    // 장바구니에 상품 추가
    @PostMapping("/carts")
    public ResponseEntity<CartResponse> addToCart(@RequestBody CartRequest cartRequest) {
        CartResponse cart = cartService.addToCart(cartRequest);
        return ResponseEntity
                .status(HttpStatus.CREATED)
                .body(cart);
    }

    // 장바구니에 상품 삭제
    @DeleteMapping("/carts/{cartId}")
    public ResponseEntity<String> deleteCart(@PathVariable Long cartId) {
        //
        cartService.deleteCart(cartId);
        return ResponseEntity.ok("장바구니에서 성공적으로 삭제되었습니다.");
    }

    // 장바구니에 담긴 상품 수량 증가
    @PatchMapping("/carts/{cartId}/increment")
    public ResponseEntity<Cart> increamentQuantityCart(@PathVariable Long cartId) {
        Cart incremented = cartService.incrementQuantity(cartId);
        return ResponseEntity.status(HttpStatus.OK).body(incremented);
    }

    // 장바구니에 담긴 상품 수량 감소
    @PatchMapping ("/carts/{cartId}/decreasement")
    public ResponseEntity<Cart> removeFromCart(@PathVariable Long cartId) {
        Cart decreasemented = cartService.decreaseQuantity(cartId);
        return ResponseEntity.status(HttpStatus.OK).body(decreasemented);
    }

    // 장바구니에 담긴 상품들의 모든 금액 합산
    @GetMapping("/carts/totalPrice")
    public ResponseEntity<Integer> getTotalPrice() {
        int totalPrice = cartService.calculateTotalPrice();
        return ResponseEntity.status(HttpStatus.OK).body(totalPrice);
    }

//    @PutMapping("/{cartId}/quantity")
//    public ResponseEntity<CartResponse> updateQuantity(@PathVariable Long cartId, @RequestParam int quantity) {
//        CartResponse cart = cartService.updateQuantity(cartId, quantity);
//        return ResponseEntity.status(HttpStatus.OK).body(cart);
//    }

//    @PostMapping("/{userId}/carts")
//    public ResponseEntity<ResponseCart> createOrder(@PathVariable("userId") String userId, @RequestBody RequestCart requestCart) {
//        ModelMapper mapper = new ModelMapper();
//        mapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//
//        CartDto cartDto = mapper.map(requestCart, CartDto.class);
//        cartDto.setUserId(userId);
//
//        CartDto response = cartService.createCart(cartDto);
//
//        ResponseCart responseCart = mapper.map(response, ResponseCart.class);
//
//        return ResponseEntity.status(HttpStatus.CREATED).body(responseCart);
//    }
//
//    @GetMapping("{userId}/carts")
//    public ResponseEntity<List<CartResponse>> getCart(@PathVariable("userId") String userId) {
//        ModelMapper modelMapper = new ModelMapper();
//
//        return ResponseEntity.status(HttpStatus.OK).body(cartService.getCartByUserId(userId));
//    }
//
//    @GetMapping("/carts")
//    public ResponseEntity<List<ResponseCart>> findAllOrders(){
//        List<CartDto> carts = cartService.getAllCarts();
//        List<ResponseCart> cartResponses = new ArrayList<>();
//        ModelMapper modelMapper = new ModelMapper();
//        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//        carts.forEach(v -> cartResponses.add(
//                modelMapper.map(v, ResponseCart.class)
//        ));
//
//
//        return ResponseEntity
//                .status(HttpStatus.OK)
//                .body(cartResponses);
//    }
}
