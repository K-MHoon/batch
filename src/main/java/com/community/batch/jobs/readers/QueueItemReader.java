package com.community.batch.jobs.readers;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * 큐를 사용해 저장하는 ItemReader 구현체
 * ItemReader의 기본 반환 타입은 단수형인데, 그에 따라 구현하면 User 객체 1개씩 DB에 select 쿼리를 요청하기 때문에,
 * 매우 비효율적인 방식이 될 수 있다.
 * @param <T>
 */
public class QueueItemReader<T> implements ItemReader<T> {
    private Queue<T> queue;

    // 휴면 회원으로 지정될 타깃 데이터를 한번에 불러와 큐에 담는다.
    public QueueItemReader(List<T> data) {
        this.queue = new LinkedList<>(data);
    }

    // 큐의 데이터를 하나씩 반환한다.
    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        return this.queue.poll();
    }
}
