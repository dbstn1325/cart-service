package tbook.cartService.service.response;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 공통 Response - 성공, 실패 여부 (Code, Message)
 */

@Getter
@AllArgsConstructor
public enum CommonResponse {

    SUCCESS(0, "Success"),
    FAIL(-1, "Fail");

    private int code;
    private String message;

}
