package com.isaolmez.springbatchguide.lesson9;

import com.isaolmez.springbatchguide.shared.StringItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.listener.ExecutionContextPromotionListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class Lesson9Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson9Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job getJob() {
        return jobBuilderFactory.get(getClass().getSimpleName())
                .start(step())
                .next(getDecider()).on("X").to(successStep())
                .from(getDecider()).on("Y").to(failureStep())
                .end()
                .build();
    }

    private JobExecutionDecider getDecider() {
        return new JobExecutionDecider() {
            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                return stepExecution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode()) ? new FlowExecutionStatus("X") : new FlowExecutionStatus("Y");
            }
        };
    }

    private Step step() {
        StringItemReader stringItemReader = new StringItemReader(5);
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(1)
                .reader(stringItemReader)
                .writer(new ItemWriter<String>() {

                    private StepExecution stepExecution;

                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        Thread.sleep(100);
                        log.info("Writing: {}", list);
                        ExecutionContext stepExecutionContext = stepExecution.getExecutionContext();
                        stepExecutionContext.put("processed", list);
                    }

                    @BeforeStep
                    public void saveStepExecution(StepExecution stepExecution) {
                        this.stepExecution = stepExecution;
                    }
                })
                .listener(promotionListener())
                .build();
    }

    private ExecutionContextPromotionListener promotionListener() {
        final ExecutionContextPromotionListener listener = new ExecutionContextPromotionListener();
        listener.setKeys(new String[] {"processed"});
        return listener;
    }

    private Step successStep() {
        return stepBuilderFactory.get("successStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        log.info("In the success step.");
                        Map<String, Object> jobExecutionContext = chunkContext.getStepContext().getJobExecutionContext();
                        log.info("Following are processed: {}", jobExecutionContext.get("processed"));
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }

    private Step failureStep() {
        return stepBuilderFactory.get("failureStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        log.info("In the failure step.");
                        return RepeatStatus.FINISHED;
                    }
                })
                .build();
    }
}
