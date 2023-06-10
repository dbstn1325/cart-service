package tbook.cartService.service.response;

import org.springframework.stereotype.Service;
import tbook.cartService.model.response.CommonResult;
import tbook.cartService.model.response.ListResult;
import tbook.cartService.model.response.SingleResult;

import java.util.List;

@Service
public class ResponseService {

    /**
     * List 타입 데이터의 응답 메시지를 가공하는 메서드
     * @param list List 타입의 DTO
     * @param <T> DTO 타입
     * @return DTO를 포함한 성공 응답
     */
    public <T>ListResult<T> getListResult(List<T> list){
        ListResult<T> result = new ListResult<>();
        result.setList(list);
        addSuccessResult(result);
        return result;
    }

    /**
     * 응답 모델을 성공 모델로 설정하는 메서드
     * @param result data(List or Single)를 담은 응답 모델
     */
    private void addSuccessResult(CommonResult result){
        result.setSuccess(true);
        result.setCode(CommonResponse.SUCCESS.getCode());
        result.setMessage(CommonResponse.SUCCESS.getMessage());
    }

    /**
     * 응답 모델을 성공 모델로 설정하는 메서드
     * @param result data(List or Single)를 담은 응답 모델
     */
    private void addFailResult(CommonResult result){
        result.setSuccess(false);
        result.setCode(CommonResponse.SUCCESS.getCode());
        result.setMessage(CommonResponse.SUCCESS.getMessage());
    }


    /**
     * 실패 결과만 처리하는 메소드
     * @return 실패결과
     */
    public CommonResult getFailResult(int code, String message) {
        CommonResult result = new CommonResult();
        result.setSuccess(false);
        setFailResult(result, code, message);
        return result;
    }

    /**
     * API 요청 실패 시 응답 모델을 성공 데이터로 세팅하는 메소드
     * @param result DTO를 담은 result 객체
     */
    private void setFailResult(CommonResult result, int code, String message){
        result.setSuccess(false);
        result.setCode(code);
        result.setMessage(message);
    }
}
