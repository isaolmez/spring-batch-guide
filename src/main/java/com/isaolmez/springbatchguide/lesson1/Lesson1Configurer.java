package com.isaolmez.springbatchguide.lesson1;

import com.isaolmez.springbatchguide.shared.DefaultJobExecutionListener;
import com.isaolmez.springbatchguide.shared.DefaultStepExecutionListener;
import com.isaolmez.springbatchguide.shared.StringItemReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
public class Lesson1Configurer {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    public Lesson1Configurer(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    public Job getJob(){
        return jobBuilderFactory.get(getClass().getSimpleName())
                .start(step())
                .listener(new DefaultJobExecutionListener())
                .build();
    }

    private Step step(){
        final StringItemReader stringItemReader = new StringItemReader();
        return stepBuilderFactory.get("step1")
                .<String,String>chunk(1)
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
