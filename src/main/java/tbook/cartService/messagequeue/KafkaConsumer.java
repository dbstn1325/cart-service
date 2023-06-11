package tbook.cartService.messagequeue;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tbook.cartService.dto.CartDto;
import tbook.cartService.dto.CartResponse;
import tbook.cartService.entity.Cart;
import tbook.cartService.repository.CartRepository;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaConsumer {
    private final CartRepository cartRepository;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "cart-product-info", groupId = "cartProductConsumer")
    public void consumeProductId(String kafkaMessage){
        Map<Object, Object> map = new HashMap<>();
        ObjectMapper mapper = new ObjectMapper();

        try {
            map = mapper.readValue(kafkaMessage, new TypeReference<Map<Object, Object>>() {});
        } catch (JsonProcessingException e) {
            log.error("JSON 파싱 오류", e);
            return;
        }

        if (!isValidData(map)) {
            log.error("유효하지 않은 데이터: {}", map);
            return;
        }

        CartDto cartDto = CartDto.builder()
                .userId(String.valueOf(map.get("userId")))
                .productId(String.valueOf(map.get("productId")))
                .productName(String.valueOf(map.get("productName")))
                .productMadeBy(String.valueOf(map.get("productMadeBy")))
                .productImage(String.valueOf(map.get("productImage")))
                .quantity(1)
                .unitPrice(Integer.parseInt(String.valueOf(map.get("productPrice"))))
                .build();


        ModelMapper modelMapper = new ModelMapper();
        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
        modelMapper.typeMap(CartDto.class, Cart.class)
                .addMapping(CartDto::getProductId, Cart::setProductId)
                .addMapping(CartDto::getUserId, Cart::setUserId);

        Cart cart = modelMapper.map(cartDto, Cart.class);

        try {
            Cart saveCart = cartRepository.save(cart);
            log.info("Kafka Message - GET " + saveCart);
        } catch(Exception e) {
            log.error("데이터베이스 저장 오류", e);
        }

    }

    @KafkaListener(topics = "cart-info-topic")
    public void consumeRemoveCart(String kafkaMessage) throws JsonProcessingException {
        try {
            List<Map<String, String>> cartIds = objectMapper.readValue(kafkaMessage, new TypeReference<List<Map<String, String>>>() {});
            log.info(cartIds.toString());
            cartIds.forEach(cartIdMap -> cartRepository.deleteById(Long.valueOf(cartIdMap.get("cartId"))));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private boolean isValidData(Map<Object, Object> map) {
        return map.containsKey("userId") &&
                map.containsKey("productId") &&
                map.containsKey("productName") &&
                map.containsKey("productMadeBy") &&
                map.containsKey("productImage") &&
                map.containsKey("productPrice");
    }



}
