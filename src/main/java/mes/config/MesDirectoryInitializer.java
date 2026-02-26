package mes.config;

import lombok.RequiredArgsConstructor;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Component
@RequiredArgsConstructor
public class MesDirectoryInitializer {

    private final Environment env;

    @PostConstruct
    public void init() {
        createDir("mes.project-path");
        createDir("mes_form_path");
        createDir("file_temp_upload_path");
    }

    private void createDir(String key) {
        String path = env.getProperty(key);
        if (path == null || path.isBlank()) return;

        try {
            Files.createDirectories(Path.of(path));
        } catch (IOException e) {
            throw new IllegalStateException(
                    "필수 디렉토리 생성 실패: " + path, e
            );
        }
    }
}