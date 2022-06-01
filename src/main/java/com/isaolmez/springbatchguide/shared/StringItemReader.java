package com.isaolmez.springbatchguide.shared;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
public class StringItemReader implements ItemReader<String> {
    private final AtomicInteger count;

    public StringItemReader() {
        this.count = new AtomicInteger(Integer.MAX_VALUE);
    }

    public StringItemReader(int count) {
        this.count = new AtomicInteger(count);
    }

    @Override
    public String read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        if (count.get() == 0) {
            return null;
        }

        String result = String.valueOf(count.decrementAndGet());
        log.info("Reading: {}", result);
        return result;
    }
}
