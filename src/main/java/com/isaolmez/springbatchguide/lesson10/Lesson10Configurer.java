package com.isaolmez.springbatchguide.lesson10;

import com.isaolmez.springbatchguide.shared.DefaultJobExecutionListener;
import com.isaolmez.springbatchguide.shared.DefaultStepExecutionListener;
import com.isaolmez.springbatchguide.shared.StringItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class Lesson10Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson10Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job getJob() {
        return jobBuilderFactory.get(getClass().getSimpleName())
                .start(step())
                .next(step2())
                .listener(new DefaultJobExecutionListener())
                .build();
    }

    private Step step() {
        StringItemReader stringItemReader = new StringItemReader(5);
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(1)
                .reader(stringItemReader)
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        Thread.sleep(100);
                        log.info("Writing: {}", list);
                        throw new RuntimeException("Planned");
                    }
                })
                .listener(new DefaultStepExecutionListener())
                .build();
    }

    private Step step2() {
        StringItemReader stringItemReader = new StringItemReader(5);
        return stepBuilderFactory.get("step2")
                .<String, String>chunk(1)
                .reader(stringItemReader)
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        Thread.sleep(100);
                        log.info("Writing: {}", list);
                    }
                })
                .listener(new DefaultStepExecutionListener())
                .build();
    }
}
