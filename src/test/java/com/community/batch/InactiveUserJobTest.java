package com.community.batch;

import com.community.batch.domain.User;
import com.community.batch.domain.enums.UserStatus;
import com.community.batch.repository.UserRepository;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.EmbeddedDatabaseConnection;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@AutoConfigureTestDatabase(connection = EmbeddedDatabaseConnection.H2)
public class InactiveUserJobTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private UserRepository userRepository;

    @Test
    public void 휴면_회원_전환_테스트() throws Exception {
        Date nowDate = new Date();

        List<User> prevTestList = userRepository.findAll();
        System.out.println("prevTestList = ");
        for (User user : prevTestList) {
            System.out.println("user name = " + user.getName() + ", updatedDate = " + user.getUpdatedDate() + "state = " + user.getStatus().name());
        }

        JobExecution jobExecution = jobLauncherTestUtils.launchJob(
                new JobParametersBuilder().addDate("nowDate", nowDate).toJobParameters()
        );

        assertThat(BatchStatus.COMPLETED).isEqualTo(jobExecution.getStatus());
        assertThat(10).isEqualTo(userRepository.findAll().size());
        assertThat(0).isEqualTo(userRepository.findByUpdatedDateBeforeAndStatusEquals(LocalDateTime.now().minusYears(1), UserStatus.ACTIVE).size());

        List<User> finishTestList = userRepository.findAll();
        System.out.println("finishTestList = ");
        for (User user : finishTestList) {
            System.out.println("user name = " + user.getName() + ", updatedDate = " + user.getUpdatedDate() + "state = " + user.getStatus().name());
        }
    }
}
