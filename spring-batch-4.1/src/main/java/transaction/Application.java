package transaction;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        List<String> finalArgs = new ArrayList<>();
        finalArgs.add("inputFiles=/transaction*.txt");
        finalArgs.addAll(Arrays.asList(args));
        SpringApplication.run(Application.class, finalArgs.toArray(new String[finalArgs.size()]));
    }
}
