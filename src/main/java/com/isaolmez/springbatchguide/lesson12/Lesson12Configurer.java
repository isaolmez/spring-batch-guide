package com.isaolmez.springbatchguide.lesson12;

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
public class Lesson12Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson12Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job getJob() {
        return jobBuilderFactory.get(getClass().getSimpleName())
                .start(step())
                .build();
    }

    private Step step() {
        StringItemReader stringItemReader = new StringItemReader(0);
        return stepBuilderFactory.get("step1")
                .<String, String>chunk(1)
                .reader(stringItemReader)
                .writer(new ItemWriter<String>() {
                    @Override
                    public void write(List<? extends String> list) throws Exception {
                        Thread.sleep(100);
                        log.info("Writing: {}", list);
                    }
                })
                .faultTolerant().retryLimit(0)
                .build();
    }
}
