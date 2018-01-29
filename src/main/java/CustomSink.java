import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;


/**
 * 自定义 flume sink 转自：http://www.aboutyun.com/thread-23883-1-1.html
 *
 * 需求：提供一个文件名称，该名称如果有具体路径的话，需要填写路径的全名称，实现的功能就是将数据保存到该文件名称中
 *
 * 分析：用户不仅可以自定义flume的source，还可以自定义flume的sink，用户自定义sink在flume中只需要继承一个基类：AbstractSink
 * 配置文件中的source是用户在控制台telnet命令监听本地的5678端口，
 * 然后根据输入的信息，该信息根据我们自定义的sink保存到我们配置的文件中，文件中定义的a1.sinks.k1.fileName属性，就是我们自定义sink的属性，
 * 该属性可以让用户自己配置，对应的目录要提前创建好
 *
 */
public class CustomSink extends AbstractSink implements Configurable {
    /**
     * 自定义sinks类详解：
     1、用户自定义的sink实现Configurable接口，实际上是实现里面的configure(Context context)方法，主要是获取用户配置的一些信息，如果我们还有很多的属性需要用户自己设置，那么我们可以在这个方法中将用户定义的参数取出来，context类中提供了很多get方法，例如getString、getLong、getBoolean等
     2、核心的处理逻辑是在process方法中，getChannel方法在父类AbstractSink中已经实现，相当于取得输送信息到sink的Channel对象，然后它里面提供事务操作方法：getTransaction()和取出消息Event的方法：take()，这两个方法在其中很重要，取得事物对象可以保证该信息被自定义的sink成功消费，成功消费后，使用commit方法提交事务，那么Event将从channel队列中删除掉，如果没有成功消费，那么使用rollback方法进行回滚，该Event将还会保留在Channel的队列中，以便下次消费，保证消息不会出现遗漏现象
     take方法主要是取出消息Event，在flume中也可以叫做事件，然后通过getBody()方法，获得消息的详细内容，就可以进行我们的功能实现了，保存到文件或者插入到数据库等等
     3、对比自定义source和自定义sink的process方法：
     自定义Source：通过getChannelProcessor方法获得ChannelProcessor对象，然后通过processEvent方法将消息转换为flume的Event对象传递给Channel处理
     自定义sink：通过getChannel方法获得Channel对象，然后通过take方法从Channel中取出Event，然后转换为我们需要的消息数据进行处理
     */

    private final String PROP_KEY_PATH = "filename";
    private String filename;

    public void configure(Context context) {
        filename = context.getString(PROP_KEY_PATH);
    }

    public Status process() throws EventDeliveryException {
        Channel ch = getChannel();

        // get the transaction
        Transaction txn = ch.getTransaction();
        Event event = null;

        // begin transaction
        txn.begin();
        while (true) {
            event = ch.take();
            if (event != null) {
                break;
            }
        }

        try {

            System.out.println("Get event.");

            String body = new String(event.getBody());
            System.out.println("event.getBody---" + body);

            String res = body + ":" + System.currentTimeMillis() + "\r\n";

            File file = new File(filename);
            FileOutputStream outputStream = null;
            try {
                outputStream = new FileOutputStream(file, true);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            try {
                outputStream.write(res.getBytes());
                outputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            txn.close();
            return Status.READY;
        } catch (Throwable throwable) {
            txn.rollback();

            if (throwable instanceof Error) {
                throw (Error) throwable;
            } else {
                throw new EventDeliveryException(throwable);
            }
        } finally {
            txn.close();
        }
    }
    // 配置文件如下
//    # 指定Agent的组件名称
//    a1.sources = r1
//    a1.sinks = k1
//    a1.channels = c1
//
//    # 指定Flume source(要监听的路径)
//    a1.sources.r1.type = netcat
//    a1.sources.r1.bind = localhost
//    a1.sources.r1.port = 5678
//
//            # 指定Flume sink
//    a1.sinks.k1.type = com.harderxin.flume.test.MySinks
//    a1.sinks.k1.fileName = D://flume-test//sink//mysinks.txt
//
//            # 指定Flume channel
//    a1.channels.c1.type = memory
//    a1.channels.c1.capacity = 1000
//    a1.channels.c1.transactionCapacity = 100
//
//            # 绑定source和sink到channel上
//    a1.sources.r1.channels = c1
//    a1.sinks.k1.channel = c1
}
