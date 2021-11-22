package com.community.batch.jobs;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.InactiveJobExecutionDecider;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InactiveStepListener;
import com.community.batch.jobs.readers.QueueItemReader;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ObjectUtils;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

@AllArgsConstructor
@Configuration
@Slf4j
public class InactiveUserJobConfig {

    private final static int CHUNK_SIZE = 15;
    private final EntityManagerFactory entityManagerFactory;

    /**
     * 휴면회원 배치 Job 빈으로 등록
     */
    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory,
                               InactiveIJobListener inactiveIJobListener,
                               Flow inactiveJobFlow) {
        return jobBuilderFactory.get("inactiveUserJob") // 'inactiveUserJob' 이라는 이름의 JobBuilder 생성
                .preventRestart() // Job 재실행 방지
                .listener(inactiveIJobListener)
                .start(inactiveJobFlow) // 파라미터에 주입받은 휴면회원 관련 Step inactiveJobStep을 제일 먼저 실행하도록 설정하는 부분
                .end()
                .build();
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory, ListItemReader<User> inactiveUserReader, InactiveStepListener inactiveStepListener) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User> chunk(CHUNK_SIZE) // chunk의 입력, 출력 타입을 User로 설정
                // chunk 인잣값은 쓰기 시에 청크 단위로 묶어서 writer() 메서드를 실행시킬 단위를 지정한 것이다. 즉, 커밋의 단위가 10개 이다.
                .reader(inactiveUserReader)
                .processor(inactiveUserProcessor()) // reader에서 조회된 User 들을 모두 비활성화 시킨다.
                .writer(inactiveUserWriter())
                .listener(inactiveStepListener)
                .build();
    }

    @Bean
    public Flow inactiveJobFlow(Step inactiveJobStep) {
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("inactiveJobFlow");
        return flowBuilder.start(new InactiveJobExecutionDecider())
                .on(FlowExecutionStatus.FAILED.getName()).end()
                .on(FlowExecutionStatus.COMPLETED.getName()).to(inactiveJobStep).end();
    }

//    @Bean
    // StepScope를 쓰면 해당 메서드가 Step 주기에 따라 새로운 빈을 생성한다.
    // 즉, 각 Step 실행마다 새로 빈을 만들기 때문에 지연 생성이 가능하다.
    // @StepScope는 기본 프록시 모드가 반환되는 클래스 타입을 참조하기 때문에 @StepScope를 사용하면 반드시 구현된 반환 타입을 명시해 반환해야 한다.
//    @StepScope
//    public QueueItemReader<User> inactiveUserReader() {
//        List<User> oldUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE);
//        return new QueueItemReader<>(oldUsers);
//    }

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
//    public ItemWriter<User> inactiveUserWriter() {
//        return ((List<? extends User> users) -> userRepository.saveAll(users));
//    }

    /**
     * ItemReader 구현체
     * JobParameter 활용
     * @return
     */
    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(@Value("#{jobParameters[nowDate]}") Date nowDate, UserRepository userRepository) {
        if(ObjectUtils.isEmpty(nowDate)) nowDate = new Date();
        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());
        List<User> inactiveUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(now.minusYears(1), UserStatus.ACTIVE);
        return new ListItemReader<>(inactiveUsers);
    }

    /**
     * JpaPagingItemReader
     * @return
     */
    // 스프링에서 destroyMethod를 사용해 삭제할 빈을 자동으로 추적한다.
    // destroyMethod=""와 같이 사용하여, 기능을 사용하지 않도록 설정하면 실행 시 warning 메시지를 삭제할 수 있다.
    @Bean(destroyMethod = "")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader() {
        /**
         * JpaPagingItemReader를 사용할 경우, Limit 인덱스 시작점이 바뀌는 현상이 있다.
         * 따라서, 조회용 인덱스값을 항상 0으로 반환하여 문제를 해결한다.
         */
        JpaPagingItemReader<User> jpaPagingItemReader = new JpaPagingItemReader(){
            @Override
            public int getPage() {
                return 0;
            }
        };
        jpaPagingItemReader.setQueryString("select u from User u where u.updatedDate < :updatedDate and u.status = :status");
        HashMap<String, Object> map = new HashMap<>();
        LocalDateTime now = LocalDateTime.now();
        map.put("updatedDate", now.minusYears(1));
        map.put("status", UserStatus.ACTIVE);

        jpaPagingItemReader.setParameterValues(map);
        jpaPagingItemReader.setEntityManagerFactory(entityManagerFactory);
        jpaPagingItemReader.setPageSize(CHUNK_SIZE);
        return jpaPagingItemReader;
    }

    /**
     * 제네릭에 저장할 타입을 명시하고 EntityManagerFactory만 설정하면 Processor에서 넘어온 데이터를 청크 단위로 저장한다.
     * @return
     */
    private JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
