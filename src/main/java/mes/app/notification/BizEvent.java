package mes.app.notification;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class BizEvent {

    private String domain;
    private String action;
    private Map<String, Object> payload;
    private String spjangcd;
    private String username;

    public Integer getPayloadInt(String key) {
        Object v = payload != null ? payload.get(key) : null;
        return v == null ? null : Integer.valueOf(v.toString());
    }
}