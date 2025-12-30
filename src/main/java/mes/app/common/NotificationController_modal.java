package mes.app.common;

import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Controller;

import java.util.HashMap;
import java.util.Map;

@Controller
@RequiredArgsConstructor
public class NotificationController_modal {

    private final SimpMessagingTemplate messagingTemplate;

    // 서버 → 클라이언트 전송
    public void sendJobOrderNotification(String message, Integer jobResId, String materialName, Float orderQty, Integer factoryId) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("message", message);
        payload.put("jobResId", jobResId);
        payload.put("materialName", materialName);
        payload.put("orderQty", orderQty);
        payload.put("factoryId", factoryId);

        messagingTemplate.convertAndSend("/topic/joborder", payload);
    }

    // 클라이언트 → 서버 메시지 테스트용
    @MessageMapping("/hello")
    public void receiveFromClient(String msg) {
//        System.out.println("Client message: " + msg);
    }
}
