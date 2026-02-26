package mes.app.aop;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;


@Component
public class ControllerExecutionTimeAspect implements Filter {

    Logger log = LoggerFactory.getLogger("API_EXEC_TIME_LOGGER");

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        String uri = req.getRequestURI();
        String contextPath = req.getContextPath();

        // 컨텍스트 패스 제거
        if (uri.startsWith(contextPath)) {
            uri = uri.substring(contextPath.length());
        }

        // /api 로 시작하는 요청만 측정
        if (!uri.startsWith("/api")) {
            chain.doFilter(request, response);
            return;
        }

        long start = System.currentTimeMillis();
        String method = req.getMethod();

        try {
            chain.doFilter(request, response);
        } finally {

            long end = System.currentTimeMillis();

            //TODO: 개선방향성
            /**
             * 나중에 성능병목이 오면 로그파일을 파싱해서 데이터 수집하는 것으로변경하자
             * 지금은 로그찍자마자 비동기로 저장하지만, 이것마저도 병목이 오면 그냥 로그파일에서 수집하는 것으로
             * **/
            double seconds = (end - start) / 1000.0;
            log.info("[API 실행시간] {} {} → {}초",
                    method, uri, String.format("%.3f", seconds));

            /*ApiLogEntry entry = new ApiLogEntry();
            entry.setOccurrenceTime(LocalDateTime.now().toString());
            entry.setMethod(method);
            entry.setApiAddress(uri);
            entry.setDurationSecond((long)((end - start) / 1000.0));
*/
        }
    }

}
