package mes.app.notification;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.Notification;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class BizEventListener {

    private final NotificationService notificationService;
    private final NotificationMessageResolver resolver;
    private final NotificationTargetService targetService;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void handle(BizEvent event) {

        System.out.println("🔥 BizEventListener ENTER");

        // 1️⃣ payload에서 factoryPk 추출
        Integer factoryPk = event.getPayloadInt("factoryPk");

        // 2️⃣ 알림 템플릿 생성
        Notification base = resolver.resolve(event);

        // 3️⃣ 수신자 목록 조회 (factoryPk 포함)
        List<String> receivers =
                targetService.findReceivers(
                        event.getDomain(),
                        event.getSpjangcd(),
                        factoryPk
                );

        // 4️⃣ 수신자별 알림 생성 & 저장
        for (String receiverUserId : receivers) {

            // 본인 제외
            if (receiverUserId.equals(event.getUsername())) {
                continue;
            }

            notificationService.save(base, receiverUserId);
        }

        System.out.println("🔥 Notification fan-out COMPLETE");
    }
}
