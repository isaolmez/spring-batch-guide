package com.isaolmez.springbatchguide.lesson5;

import com.isaolmez.springbatchguide.shared.DefaultJobExecutionListener;
import com.isaolmez.springbatchguide.shared.DefaultStepExecutionListener;
import com.isaolmez.springbatchguide.shared.StringItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.*;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.listener.StepExecutionListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class Lesson5Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson5Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job getJob() {
        return jobBuilderFactory.get(getClass().getSimpleName())
                .start(step())
                .listener(new DefaultJobExecutionListener())
                .on("X").to(successStep())
                .from(step()).on("Y").to(failureStep()).end()
                .build();
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
                .listener(new StepExecutionListenerSupport() {
                    @Override
                    public ExitStatus afterStep(StepExecution stepExecution) {
                        String exitCode = stepExecution.getExitStatus().getExitCode();
                        if (exitCode.equals(ExitStatus.COMPLETED.getExitCode())) {
                            return new ExitStatus("X");
                        }
                        return new ExitStatus("Y");
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
