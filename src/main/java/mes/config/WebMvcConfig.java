package mes.config;


import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
public class WebMvcConfig implements WebMvcConfigurer{

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        //registry.addInterceptor(new GuiHttpInterceptor()).addPathPatterns("/gui/*");
        //registry.addInterceptor(new ApiHttpInterceptor()).addPathPatterns("/Api/*");
    }

    @Override
    public void addCorsMappings(CorsRegistry registry){
        registry.addMapping("/**")
                .allowedOrigins(
                        "http://localhost:8030", "http://actascld.co.kr:8030/", "http://mes.actascld.co.kr", "https://mes.actascld.co.kr", // 모든 오리진 허용
                        "http://localhost:8031", "http://actascld.co.kr:8031/", "http://dy.actascld.co.kr", "https://dy.actascld.co.kr", "https://shin.actascld.co.kr", "http://actascld.co.kr:8034/"
                )
                .allowedMethods("GET", "POST", "PUT", "DELETE", "HEAD")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
    

}
