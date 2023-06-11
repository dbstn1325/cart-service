package tbook.cartService.utils;

import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;
import tbook.cartService.dto.CartDto;
import tbook.cartService.entity.Cart;

public class CartMapperUtil {

    private static final ModelMapper modelMapper = new ModelMapper();

    static {
        modelMapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);
        modelMapper.typeMap(CartDto.class, Cart.class)
                .addMapping(CartDto::getProductId, Cart::setProductId)
                .addMapping(CartDto::getUserId, Cart::setUserId);
    }

    public static Cart convertToCartEntity(CartDto cartDto) {
        return modelMapper.map(cartDto, Cart.class);
    }
}
