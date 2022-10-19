package org.joisen.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author : joisen
 * @date : 11:05 2022/10/12
 */
public class MySink extends AbstractSink implements Configurable {

    private String prefix;
    private String subfix;
    //创建logger对象
    private Logger logger = LoggerFactory.getLogger(MySink.class);


    @Override
    public void configure(Context context) {
        prefix = context.getString("pre","pre-");
        subfix = context.getString("sub");
    }
    @Override
    public Status process() throws EventDeliveryException {
        // 1.获取Channel并开启事务
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();

        // 2从Channel中抓取数据打印到控制台
        try {
            // 2.1 抓取数据
            Event event;
            while (true){
                event = channel.take();
                if (event != null){
                    break;
                }
            }
            // 2.2 处理数据
            logger.info(prefix + new String(event.getBody())+subfix);

            // 2.3 提交事务
            transaction.commit();
            return Status.READY;

        }catch (Exception e){
            // 回滚
            transaction.rollback();
            return Status.BACKOFF;
        }finally {
            transaction.close();
        }
    }

}
