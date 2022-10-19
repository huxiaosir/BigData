package org.joisen.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.function.IntPredicate;

/**
 * @author : joisen
 * @date : 14:39 2022/10/3
 */
public class MyUDF extends GenericUDF {
    // 初始化  校验数据参数个数

    /**
     * @param arguments 输入参数类型的鉴别器对象
     * @return 返回值类型的鉴别器对象
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if(arguments.length != 1){
            throw new UDFArgumentException("参数个数不为 1");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 函数的逻辑处理
     * @param deferredObjects 输入的参数
     * @return 返回值
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        // 1.取出输入数据
        String input = deferredObjects[0].get().toString();
        // 2.判断输入数据是否为null
        if(input == null) return 0;
        return input.length();
    }
    //
    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}
