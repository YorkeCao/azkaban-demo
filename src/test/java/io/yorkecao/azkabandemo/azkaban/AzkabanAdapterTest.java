package io.yorkecao.azkabandemo.azkaban;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

import static org.junit.Assert.*;

@SpringBootTest
@RunWith(SpringRunner.class)
public class AzkabanAdapterTest {

    @Autowired
    private AzkabanAdapter azkabanAdapter;

    @Test
    public void schedulePeriodBasedFlow() {
        String projectName = "PROJECT_NAME";
        String flowName = "FLOW_NAME";
        String scheduleDate = "07/22/2014";
        String scheduleTime = "12,00,pm,PDT";
        String period = "5w";

        try {
            azkabanAdapter.schedulePeriodBasedFlow(projectName, flowName, scheduleDate, scheduleTime, period);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void scheduleFlow() {
        String projectName = "wtwt";
        String flowName = "azkaban-training";
        String cronExpression = "0 23/30 5,7-10 ? * 6#3";

        try {
            azkabanAdapter.scheduleFlow(projectName, flowName, cronExpression);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}