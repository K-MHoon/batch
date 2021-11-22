package com.community.batch.jobs;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.readers.QueueItemReader;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.List;

@AllArgsConstructor
@Configuration
public class InactiveUserJobConfig {

    private UserRepository userRepository;

    /**
     * 휴면회원 배치 Job 빈으로 등록
     */
    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory, Step inactiveJobStep) {
        return jobBuilderFactory.get("inactiveUserJob") // 'inactiveUserJob' 이라는 이름의 JobBuilder 생성
                .preventRestart() // Job 재실행 방지
                .start(inactiveJobStep) // 파라미터에 주입받은 휴면회원 관련 Step inactiveJobStep을 제일 먼저 실행하도록 설정하는 부분
                .build();
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User> chunk(10) // chunk의 입력, 출력 타입을 User로 설정
                // chunk 인잣값은 쓰기 시에 청크 단위로 묶어서 writer() 메서드를 실행시킬 단위를 지정한 것이다. 즉, 커밋의 단위가 10개 이다.
                .reader(inactiveUserReader())
                .processor(inactiveUserProcessor()) // reader에서 조회된 User 들을 모두 비활성화 시킨다.
                .writer(inactiveUserWriter())
                .build();
    }

    @Bean
    // StepScope를 쓰면 해당 메서드가 Step 주기에 따라 새로운 빈을 생성한다.
    // 즉, 각 Step 실행마다 새로 빈을 만들기 때문에 지연 생성이 가능하다.
    // @StepScope는 기본 프록시 모드가 반환되는 클래스 타입을 참조하기 때문에 @StepScope를 사용하면 반드시 구현된 반환 타입을 명시해 반환해야 한다.
    @StepScope
    public QueueItemReader<User> inactiveUserReader() {
        List<User> oldUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE);
        return new QueueItemReader<>(oldUsers);
    }

    public ItemProcessor<User, User> inactiveUserProcessor() {
         return item -> item.setInactive();
//        return new ItemProcessor<User, User>() {
//            @Override
//            public User process(User item) throws Exception {
//                return item.setInactive();
//            }
//        };
    }

    // 청크 단위를 10으로 설정했으므로 users에는 휴면회원 10개가 주어지며 saveAll() 메서드를 사용해서 한번에 DB에 저장한다.
    public ItemWriter<User> inactiveUserWriter() {
        return ((List<? extends User> users) -> userRepository.saveAll(users));
    }
}
