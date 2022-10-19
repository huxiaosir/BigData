package org.joisen.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author : joisen
 * @date : 9:03 2022/10/12
 */
public class TypeInterceptor implements Interceptor {
    // 声明一个用于存放拦截器处理后的事件的集合
    private List<Event> addHeaderEvents;
    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();
    }
    // 单个事件处理方法
    @Override
    public Event intercept(Event event) {
        // 1.获取header、body
        Map<String,String> header = event.getHeaders();
        String body = new String(event.getBody());

        // 2.判断body中是否包含‘joisen’ 而添加不同的头信息
        if(body.contains("joisen")){
            header.put("type","joisen");
        }else{
            header.put("type","other");
        }
        // 3. 返回数据
        return event;
    }
    // 批量事件处理方法
    @Override
    public List<Event> intercept(List<Event> list) {
        // 1.清空集合
        addHeaderEvents.clear();
        // 2.遍历events
        for (Event event : list) {
            addHeaderEvents.add(intercept(event));
        }
        // 3. 返回数据
        return addHeaderEvents;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}
