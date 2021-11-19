package com.community.batch;

import com.community.batch.domain.User;
import com.community.batch.repository.UserRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class BatchApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchApplication.class, args);
	}

	@Bean
	public CommandLineRunner runner(UserRepository userRepository) throws Exception {
		return (args) -> {
			userRepository.save(User.builder()
					.name("havi")
					.password("test")
					.email("havi@gmail.com")
					.build());
			userRepository.save(User.builder()
					.name("havi2")
					.password("test2")
					.email("havi@gmail.com2")
					.build());
			userRepository.save(User.builder()
					.name("havi3")
					.password("test3")
					.email("havi@gmail.com3")
					.build());

		};
	}

}
