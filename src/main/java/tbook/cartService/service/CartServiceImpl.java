package tbook.cartService.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import tbook.cartService.dto.CartGetProductRequest;
import tbook.cartService.dto.CartRequest;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;
import tbook.cartService.messagequeue.KafkaProducer;
import tbook.cartService.model.response.ListResult;
import tbook.cartService.repository.CartRepository;
import tbook.cartService.service.response.ResponseService;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartServiceImpl implements CartService {
    private final CartRepository cartRepository;
    private final KafkaProducer kafkaProducer;
    private final ResponseService responseService;
    private final ModelMapper modelMapper;

    // 모든 장바구니 조회
    public List<CartResponse> getAllCartsByUserId(String userId) {
        List<Cart> carts = cartRepository.findByUserId(userId)
                .orElseThrow(()-> new IllegalArgumentException("잘못된 접근입니다."));

        return carts.stream()
                .map(cart -> modelMapper.map(cart, CartResponse.class))
                .collect(Collectors.toList());
    }

    // 장바구니에 상품 추가
    public void createCart(CartGetProductRequest cartGetProductRequest) {
        kafkaProducer.sendMessage(cartGetProductRequest);
    }

    // 장바구니에 담긴 상품 수량 증가
    public Cart incrementQuantity(Long cartId) {
        Cart cart = cartRepository.findById(cartId)
                .orElseThrow(() -> new IllegalArgumentException("올바르지 않은 cartId 입니당.^^"));
        cart.setQuantity(cart.getQuantity() + 1);
//        cart.setTotalPrice(cart.getUnitPrice() * cart.getQuantity());
        return cartRepository.save(cart);
    }

    // 장바구니에 담긴 상품 수량 감소
    public Cart decreaseQuantity(Long cartId) {
        Cart cart = cartRepository.findById(cartId)
                .orElseThrow(() -> new IllegalArgumentException("올바르지 않은 cartId 입니당.^^"));
        int newQuantity = cart.getQuantity() - 1;
        if(newQuantity <= 0) {
            cartRepository.delete(cart);
            return null;
        }

        cart.setQuantity(newQuantity);
//        cart.setTotalPrice(cart.getUnitPrice() * cart.getQuantity());
        return cartRepository.save(cart);
    }

    // 장바구니에 담긴 상품 삭제
    public void deleteCart(Long cartId) {
        cartRepository.deleteById(cartId);
    }

    // 장바구니에 담긴 상품들의 모든 금액 합산
    @Override
    public int calculateTotalPrice() {
        List<Cart> cartsAll = cartRepository.findAll();
        int totalPrice = 0;
        for (Cart cart : cartsAll) {
//            totalPrice += cart.getTotalPrice();
        }

        return totalPrice;
    }

    // cartResponse로 매핑해주기 위한 함수(있어도 되고 없어도 됨)
    private CartResponse mapToCartResponse(Cart cart) {
        CartResponse response = new CartResponse();
        response.setProductName(cart.getProductName());
        response.setQuantity(cart.getQuantity());
        response.setUnitPrice(cart.getUnitPrice());
//        response.setTotalPrice(cart.getTotalPrice());
        return response;
    }


//    @Override
//    public CartDto createCart(CartDto cartDto) {
//        cartDto.setCartId(UUID.randomUUID().toString());
//        cartDto.setTotalPrice(cartDto.getQuantity() * cartDto.getUnitPrice());
//        ModelMapper modelMapper = new ModelMapper();
//        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//        Cart cart = modelMapper.map(cartDto, Cart.class);
//
//        cartRepository.save(cart);
//
//        CartDto resultCartDto = modelMapper.map(cart, CartDto.class);
//
//        return resultCartDto;
//    }
//
//    @Override
//    public CartDto getCartByCartId(String cartId) {
//        Cart cart = cartRepository.findByCartId(cartId);
//
//        ModelMapper modelMapper = new ModelMapper();
//        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//        CartDto cartDto = modelMapper.map(cart, CartDto.class);
//
//        return cartDto;
//    }
//
//    @Override
//    public List<CartResponse> getCartByUserId(String userId) {
//        List<Cart> carts = cartRepository.findByUserId(userId);
//
//        List<CartResponse> responses = new ArrayList<>();
//
//        ModelMapper modelMapper = new ModelMapper();
//        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//        carts.forEach(v -> responses.add(modelMapper.map(v, CartResponse.class)));
//
//        return responses;
//    }
//
//    @Override
//    public List<CartDto> getAllCarts() {
//        List<Cart> orders = cartRepository.findAll();
//
//        List<CartDto> orderDtos = new ArrayList<>();
//        ModelMapper modelMapper = new ModelMapper();
//        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
//        orders.forEach(v -> orderDtos.add(
//                modelMapper.map(v, CartDto.class)
//        ));
//
//        return orderDtos;
//    }
}
