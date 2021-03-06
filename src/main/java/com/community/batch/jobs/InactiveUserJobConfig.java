package com.community.batch.jobs;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.Grade;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.jobs.inactive.InactiveJobExecutionDecider;
import com.community.batch.jobs.inactive.InactiveUserPartitioner;
import com.community.batch.jobs.inactive.listener.InactiveIJobListener;
import com.community.batch.jobs.inactive.listener.InactiveStepListener;
import com.community.batch.jobs.readers.QueueItemReader;
import com.community.batch.repository.UserRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
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
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.util.ObjectUtils;

import javax.persistence.EntityManagerFactory;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.stream.IntStream;

@AllArgsConstructor
@Configuration
@Slf4j
public class InactiveUserJobConfig {

    private final static int CHUNK_SIZE = 15;
    private final EntityManagerFactory entityManagerFactory;

    /**
     * ???????????? ?????? Job ????????? ?????? by MultiFlow
     */
//    @Bean
//    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory,
//                               InactiveIJobListener inactiveIJobListener,
//                               Flow multiFlow) {
//        return jobBuilderFactory.get("inactiveUserJob") // 'inactiveUserJob' ????????? ????????? JobBuilder ??????
//                .preventRestart() // Job ????????? ??????
//                .listener(inactiveIJobListener)
//                .start(multiFlow) // ??????????????? ???????????? ???????????? ?????? Step inactiveJobStep??? ?????? ?????? ??????????????? ???????????? ??????
//                .end()
//                .build();
//    }

    /**
     * ???????????? ?????? Job ????????? ?????? by Partition
     */
    @Bean
    public Job inactiveUserJob(JobBuilderFactory jobBuilderFactory,
                               InactiveIJobListener inactiveIJobListener,
                               Step partitionerStep) {
        return jobBuilderFactory.get("inactiveUserJob") // 'inactiveUserJob' ????????? ????????? JobBuilder ??????
                .preventRestart() // Job ????????? ??????
                .listener(inactiveIJobListener)
                .start(partitionerStep) // ??????????????? ???????????? ???????????? ?????? Step inactiveJobStep??? ?????? ?????? ??????????????? ???????????? ??????
                .build();
    }

    @Bean
    @JobScope // Job ?????? ????????? ?????? ?????? ???????????? @JobScope ??????
    public Step partitionerStep(StepBuilderFactory stepBuilderFactory, Step inactiveJobStep) {
        return stepBuilderFactory
                .get("partitionerStep")
                .partitioner("partitionerStep", new InactiveUserPartitioner())
                .gridSize(3) // ??????????????? ????????? gridSize, ?????? Grade Enum??? 3????????? 3 ???????????? ?????? ?????????.
                .step(inactiveJobStep)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public Flow multiFlow(Step inactiveJobStep) {
        Flow[] flows = new Flow[5];

        // IntStream??? ????????? ????????? ?????? ?????? ???????????? ?????????.
        // FlowBuilder ????????? Flow??? 5??? ???????????? flows ????????? ????????????.
        IntStream.range(0, flows.length).forEach(i ->
                flows[i] = new FlowBuilder<Flow>("MultiFlow"+i)
                        .from(inactiveJobFlow(inactiveJobStep))
                        .end());
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("MultiFlowTest");
        return flowBuilder
                .split(taskExecutor()) // multiFlow?????? ????????? TaskExecutor??? ????????????.
                .add(flows) // inactiveJobFlow 5?????? ????????? flows ????????? ????????????.
                .build();
    }

    /**
     * ?????? ????????? Step
     * @return
     */
    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor("Batch_Task"); // ?????? ????????? 1??? ??????????????? ????????? ?????????
    }

    @Bean
    public Step inactiveJobStep(StepBuilderFactory stepBuilderFactory, ListItemReader<User> inactiveUserReader, InactiveStepListener inactiveStepListener, TaskExecutor taskExecutor) {
        return stepBuilderFactory.get("inactiveUserStep")
                .<User, User> chunk(CHUNK_SIZE) // chunk??? ??????, ?????? ????????? User??? ??????
                // chunk ???????????? ?????? ?????? ?????? ????????? ????????? writer() ???????????? ???????????? ????????? ????????? ?????????. ???, ????????? ????????? 10??? ??????.
                .reader(inactiveUserReader)
                .processor(inactiveUserProcessor()) // reader?????? ????????? User ?????? ?????? ???????????? ?????????.
                .writer(inactiveUserWriter())
                .listener(inactiveStepListener)
                .taskExecutor(taskExecutor)
                .throttleLimit(2) // ????????? ?????? ???????????? ???????????? ????????? ????????????????????? ??????. ???????????? ????????? ????????? ?????? ???????????? ?????? ????????? ??????????????? ???
                .build();
    }

//    @Bean
    public Flow inactiveJobFlow(Step inactiveJobStep) {
        FlowBuilder<Flow> flowBuilder = new FlowBuilder<>("inactiveJobFlow");
        return flowBuilder.start(new InactiveJobExecutionDecider())
                .on(FlowExecutionStatus.FAILED.getName()).end()
                .on(FlowExecutionStatus.COMPLETED.getName()).to(inactiveJobStep).end();
    }

//    @Bean
    // StepScope??? ?????? ?????? ???????????? Step ????????? ?????? ????????? ?????? ????????????.
    // ???, ??? Step ???????????? ?????? ?????? ????????? ????????? ?????? ????????? ????????????.
    // @StepScope??? ?????? ????????? ????????? ???????????? ????????? ????????? ???????????? ????????? @StepScope??? ???????????? ????????? ????????? ?????? ????????? ????????? ???????????? ??????.
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

    // ?????? ????????? 10?????? ?????????????????? users?????? ???????????? 10?????? ???????????? saveAll() ???????????? ???????????? ????????? DB??? ????????????.
//    public ItemWriter<User> inactiveUserWriter() {
//        return ((List<? extends User> users) -> userRepository.saveAll(users));
//    }

    /**
     * ItemReader ?????????
     * JobParameter ??????
     * @return
     */
//    @Bean
//    @StepScope
//    public ListItemReader<User> inactiveUserReader(@Value("#{jobParameters[nowDate]}") Date nowDate, UserRepository userRepository) {
//        if(ObjectUtils.isEmpty(nowDate)) nowDate = new Date();
//        LocalDateTime now = LocalDateTime.ofInstant(nowDate.toInstant(), ZoneId.systemDefault());
//        List<User> inactiveUsers = userRepository.findByUpdatedDateBeforeAndStatusEquals(now.minusYears(1), UserStatus.ACTIVE);
//        return new ListItemReader<>(inactiveUsers);
//    }

    @Bean
    @StepScope
    public ListItemReader<User> inactiveUserReader(@Value("#{stepExecutionContext[grade]}") String grade, UserRepository userRepository) {
        log.info(Thread.currentThread().getName());
        List<User> inactiveUsers = userRepository.findByUpdatedDateBeforeAndStatusEqualsAndGradeEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE, Grade.valueOf(grade));
        return new ListItemReader<>(inactiveUsers);
    }

    /**
     * JpaPagingItemReader
     * @return
     */
    // ??????????????? destroyMethod??? ????????? ????????? ?????? ???????????? ????????????.
    // destroyMethod=""??? ?????? ????????????, ????????? ???????????? ????????? ???????????? ?????? ??? warning ???????????? ????????? ??? ??????.
    @Bean(destroyMethod = "")
    @StepScope
    public JpaPagingItemReader<User> inactiveUserJpaReader() {
        /**
         * JpaPagingItemReader??? ????????? ??????, Limit ????????? ???????????? ????????? ????????? ??????.
         * ?????????, ????????? ??????????????? ?????? 0?????? ???????????? ????????? ????????????.
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
     * ???????????? ????????? ????????? ???????????? EntityManagerFactory??? ???????????? Processor?????? ????????? ???????????? ?????? ????????? ????????????.
     * @return
     */
    private JpaItemWriter<User> inactiveUserWriter() {
        JpaItemWriter<User> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
