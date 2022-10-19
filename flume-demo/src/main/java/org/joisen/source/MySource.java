package org.joisen.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

/**
 * @author : joisen
 * @date : 10:05 2022/10/12
 */
public class MySource extends AbstractSource implements Configurable, PollableSource {

    private String prefix;
    private String subfix;
    private Long delay;

    @Override
    public void configure(Context context) {

        prefix = context.getString("pre","pre-");
        subfix = context.getString("sub");
        delay = context.getLong("delay",2000L);
    }
    @Override
    public Status process() throws EventDeliveryException {

        // 2.循环创建事件信息，传给channel
        try {
            for(int i=0;i<5;i++){
                // 1. 声明事件
                Event event = new SimpleEvent();
                HashMap<String, String> header = new HashMap<>();
                event.setHeaders(header);
                event.setBody((prefix+"joisen:"+i+subfix).getBytes());
                getChannelProcessor().processEvent(event);
            }
            Thread.sleep(delay);
            return Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }

}
