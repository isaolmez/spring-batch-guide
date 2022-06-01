package com.isaolmez.springbatchguide.lesson6;

import com.isaolmez.springbatchguide.shared.StringItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class Lesson6Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson6Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
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
        return (jobExecution, stepExecution) -> stepExecution.getExitStatus().getExitCode().equals(ExitStatus.COMPLETED.getExitCode()) ? new FlowExecutionStatus("X") : new FlowExecutionStatus("Y");
    }

    private Step step() {
        StringItemReader stringItemReader = new StringItemReader(5);
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(10)
                .reader(stringItemReader)
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        Thread.sleep(100);
                        log.info("Writing: {}", list);
                    }
                })
                .build();
    }

    private Step successStep() {
        return stepBuilderFactory.get("successStep")
                .tasklet(new Tasklet() {
                    @Override
                    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                        log.info("In the success step.");
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
