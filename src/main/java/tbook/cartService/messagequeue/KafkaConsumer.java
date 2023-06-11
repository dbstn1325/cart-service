package tbook.cartService.messagequeue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tbook.cartService.dto.CartDto;
import tbook.cartService.entity.Cart;
import tbook.cartService.repository.CartRepository;
import tbook.cartService.utils.CartMapperUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumer {

    private final CartRepository cartRepository;
    private final ObjectMapper objectMapper;

    /**
     * Kafka로부터 'cart-product-info' 토픽의 메시지를 소비하여 장바구니 정보를 처리
     * @param kafkaMessage JSON 형태의 문자열
     *                     - userId, productId, productName, productMadeBy, productImage, productPrice
     */
    @KafkaListener(topics = "cart-product-info", groupId = "cartProductConsumer")
    public void consumeProductId(String kafkaMessage) {
        Map<Object, Object> productData = parseKafkaMessage(kafkaMessage);

        if (productData == null || !isValidData(productData)) {
            log.error("유효하지 않은 데이터: {}", productData);
            return;
        }

        String userId = String.valueOf(productData.get("userId"));
        String productId = String.valueOf(productData.get("productId"));

        List<Cart> existingCarts = cartRepository.findByUserIdAndProductId(userId, productId);

        if (!existingCarts.isEmpty()) {
            updateExistingCart(existingCarts.get(0));
        } else {
            saveNewCart(productData);
        }
    }

    /**
     * Kafka로부터 'cart-info-topic' 토픽의 메시지를 소비하여 장바구니 삭제 처리를 수행
     * @param kafkaMessage JSON 형태의 문자열 - 장바구니의 ID 목록
     */
    @KafkaListener(topics = "cart-info-topic")
    public void consumeRemoveCart(String kafkaMessage) {
        try {
            List<Map<String, String>> cartIds = objectMapper.readValue(kafkaMessage, new TypeReference<List<Map<String, String>>>() {});
            cartIds.forEach(cartIdMap -> cartRepository.deleteById(Long.valueOf(cartIdMap.get("cartId"))));
        } catch (IOException e) {
            log.error("JSON 파싱 오류", e);
        }
    }

    /**
     * Kafka로부터 받은 메시지를 파싱하여 Map 형태로 반환
     * @param kafkaMessage JSON 형태의 문자열 - 장바구니의 ID 목록
     * @res Map<Object, Object> - 파싱된 데이터를 포함하는 Map. 파싱에 실패하면 null을 반환.
     */
    private Map<Object, Object> parseKafkaMessage(String kafkaMessage) {
        try {
            return objectMapper.readValue(kafkaMessage, new TypeReference<Map<Object, Object>>() {});
        } catch (JsonProcessingException e) {
            log.error("JSON 파싱 오류", e);
            return null;
        }
    }

    /**
     * 받은 데이터가 필요한 키를 가지고 있는지 확인하여 유효한지 판단
     * @param data 키와 값을 포함 - Map 타입
     * @return 데이터가 유효하면 true, 그렇지 않으면 false
     */
    private boolean isValidData(Map<Object, Object> data) {
        return data.containsKey("userId") &&
                data.containsKey("productId") &&
                data.containsKey("productName") &&
                data.containsKey("productMadeBy") &&
                data.containsKey("productImage") &&
                data.containsKey("productPrice");
    }

    /**
     * 기존의 장바구니 항목을 업데이트
     * @param cart 업데이트할 장바구니 항목
     */
    private void updateExistingCart(Cart cart) {
        cart.setQuantity(cart.getQuantity() + 1);
        cartRepository.save(cart);
        log.info("Kafka Message - Quantity Updated: " + cart);
    }

    /**
     * 새로운 장바구니 항목을 데이터베이스에 저장
     * @param productData 새로운 장바구니 항목 정보를 포함 - Map 타입
     */
    private void saveNewCart(Map<Object, Object> productData) {
        CartDto cartDto = CartDto.fromMap(productData);
        Cart cart = CartMapperUtil.convertToCartEntity(cartDto);

        try {
            Cart savedCart = cartRepository.save(cart);
            log.info("Kafka Message - New Cart Saved: " + savedCart);
        } catch (Exception e) {
            log.error("데이터베이스 저장 오류", e);
        }
    }

}
