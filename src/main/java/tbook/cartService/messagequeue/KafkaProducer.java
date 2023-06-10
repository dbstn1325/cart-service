package tbook.cartService.messagequeue;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import tbook.cartService.dto.CartGetProductRequest;
import tbook.cartService.dto.CartRequest;

@Service
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {
    private static final String TOPIC = "retrieve-product";
    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(CartGetProductRequest cartGetProductRequest){
        ObjectMapper ob = new ObjectMapper();
        String jsonInString = "";
        try{
            jsonInString = ob.writeValueAsString(cartGetProductRequest);
        } catch (JsonProcessingException e) {
            log.info("error in cart");
            throw new RuntimeException(e);
        }



        kafkaTemplate.send(TOPIC, jsonInString);

    }
}
