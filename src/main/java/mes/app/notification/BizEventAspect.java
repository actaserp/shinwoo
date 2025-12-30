package mes.app.notification;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

@Aspect
@Component
@RequiredArgsConstructor
public class BizEventAspect {

    private final ApplicationEventPublisher publisher;

    @Around("@annotation(trigger)")
    public Object handleBizEvent(
            ProceedingJoinPoint pjp,
            BizEventTrigger trigger
    ) throws Throwable {

        // 1️⃣ 실제 메서드 실행
        Object result = pjp.proceed();

        // 2️⃣ 인증 사용자
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        User user = (User) auth.getPrincipal();
        String spjangcd = user.getSpjangcd();

        // 3️⃣ payload 구성
        Map<String, Object> payload = new HashMap<>();

        /* =========================
         * (1) AjaxResult.data 해석
         * ========================= */
        if (result instanceof AjaxResult ar && ar.data != null) {

            Object data = ar.data;

            // ID 하나만 내려온 경우
            if (data instanceof Number) {
                payload.put("targetId", data);
            }

            // Map 내려온 경우
            else if (data instanceof Map<?, ?> map) {
                map.forEach((k, v) -> payload.put(k.toString(), v));
            }

            // Entity 내려온 경우 (ID만 추출)
            else {
                try {
                    Method getId = data.getClass().getMethod("getId");
                    Object id = getId.invoke(data);
                    payload.put("targetId", id);
                } catch (Exception ignore) {
                    // id 없는 객체면 payload에서 무시
                }
            }
        }

        /* =========================
         * (2) 메서드 인자 기반 컨텍스트
         * ========================= */
        for (Object arg : pjp.getArgs()) {
            if (arg == null) continue;

            // Integer = factoryPk 후보
            if (arg instanceof Integer i) {
                payload.putIfAbsent("factoryPk", i);
            }

            // Map 직접 전달한 경우
            if (arg instanceof Map<?, ?> map) {
                map.forEach((k, v) -> payload.putIfAbsent(k.toString(), v));
            }
        }

        /* =========================
         * AFTER_COMMIT 이벤트 발행
         * ========================= */
        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronizationAdapter() {
                    @Override
                    public void afterCommit() {

                        BizEvent event = new BizEvent(
                                trigger.domain(),
                                trigger.action(),
                                payload.isEmpty() ? null : payload,
                                spjangcd,
                                user.getUsername()
                        );

                        publisher.publishEvent(event);
                    }
                }
        );

        System.out.println("🔥 BizEventAspect HIT: "
                + trigger.domain() + " / " + trigger.action()
                + " payload=" + payload);

        return result;
    }
}
