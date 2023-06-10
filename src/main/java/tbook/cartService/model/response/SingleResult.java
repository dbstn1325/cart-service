package tbook.cartService.model.response;

import lombok.Getter;
import lombok.Setter;

/**
 * Response Model - Single
 */

@Getter
@Setter
public class SingleResult<T> extends CommonResult{

    // Response data - dto
    private T data;
}
