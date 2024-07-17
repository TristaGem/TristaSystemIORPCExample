package rpc198;


import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

class Myheader implements Serializable{
    //protocol
    /**
     * 1. ooxx
     * 2. UUID
     * 3. DATA_LEN
     */

    int flag; //32bit
    long requestID;
    long dataLen;

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public long getRequestID() {
        return requestID;
    }

    public void setRequestID(long requestID) {
        this.requestID = requestID;
    }

    public long getDataLen() {
        return dataLen;
    }

    public void setDataLen(long dataLen) {
        this.dataLen = dataLen;
    }
}

class MyContent implements Serializable {

    //请求类型
    String name;
    String methodName;
    Class<?>[] parameterTypes;
    Object[] args;

    //返回类型
    Object res;

    public Object getRes() {
        return res;
    }

    public void setRes(String res) {
        this.res = res;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public Class<?>[] getParameterTypes() {
        return parameterTypes;
    }

    public void setParameterTypes(Class<?>[] parameterTypes) {
        this.parameterTypes = parameterTypes;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }
}

//源于 spark 源码
class ClientFactory {

    int poolSize = 10;
    NioEventLoopGroup clientWorker;
    Random rand = new Random();

    private ClientFactory() {
    }

    private static final ClientFactory factory;

    static {
        factory = new ClientFactory();
    }

    public static ClientFactory getFactory() {
        return factory;
    }


    //一个consumer 可以连接很多的 provider，每一个provider都有自己的pool  K,V
    ConcurrentHashMap<InetSocketAddress, ClientPool> outboxs = new ConcurrentHashMap<>();

    public synchronized NioSocketChannel getClient(InetSocketAddress address) {

        ClientPool clientPool = outboxs.get(address);
        if (clientPool == null) {
            outboxs.putIfAbsent(address, new ClientPool(poolSize));
            clientPool = outboxs.get(address);
        }

        int i = rand.nextInt(poolSize);

        if (clientPool.clients[i] != null && clientPool.clients[i].isActive()) {
            return clientPool.clients[i];
        }

        synchronized (clientPool.lock[i]) {
            return clientPool.clients[i] = create(address);
        }

    }

    private NioSocketChannel create(InetSocketAddress address){

        //基于 netty 的客户端创建方式
//        clientWorker = ;
        Bootstrap bs = new Bootstrap();
        ChannelFuture connect = bs.group(new NioEventLoopGroup(1))
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
//                        p.addLast(new ServerDecode());
                        p.addLast(new ClientResponses());  //解决给谁的？？  requestID..
                    }
                }).connect(address);
        try {
            NioSocketChannel client = (NioSocketChannel)connect.sync().channel();
            return client;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}

class ClientPool{
    NioSocketChannel[] clients;
    Object[] lock;

    ClientPool(int size){
        clients = new NioSocketChannel[size];//init  连接都是空的
        lock = new Object[size]; //锁是可以初始化的
        for(int i = 0;i< size;i++){
            lock[i] = new Object();
        }
    }
}


interface Car{
    public String ooxx(String msg);
}

public class RPCExampleWithProtocolHandling {

    public void startServer() {
        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup worker = new NioEventLoopGroup(50);

        ServerBootstrap sbs = new ServerBootstrap();
        ChannelFuture bind = sbs.group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<NioSocketChannel>() {
                    @Override
                    protected void initChannel(NioSocketChannel ch) throws Exception {
                        System.out.println("server accept client port " + ch.remoteAddress());
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new ServerDecode());
                        pipeline.addLast(new ServerRequestHandler());
                    }
                }).bind(new InetSocketAddress("127.0.0.1", 9090));
        try {
            bind.sync().channel().closeFuture().sync();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void singleThreadGet() {
        new Thread(() -> {
            startServer();
        }).start();

        System.out.println("server started\n");

        Car car = proxyGet(Car.class);
        car.ooxx("hello");

    }

    @Test
    public void multiThreadGet() throws InterruptedException {
        new Thread(() -> {
            startServer();
        }).start();

        System.out.printf("server started");

        int size = 20;
        Thread[] threads = new Thread[size];
        for(int i = 0;i<size;i++){
            final int clientID = i;
            threads[i] = new Thread(() -> {
                Car car = proxyGet(Car.class);
                System.out.println(car.ooxx("hello from client " + clientID));
            });
        }

        for(Thread thread : threads){
            thread.start();
        }
        for(Thread thread : threads){
            thread.join();
        }
    }

    public static Myheader createHeader(byte[] msg){
        Myheader header = new Myheader();
        int size = msg.length;
        int f = 0x14141414;
        long requestID =  Math.abs(UUID.randomUUID().getLeastSignificantBits());
        //0x14  0001 0100
        header.setFlag(f);
        header.setDataLen(size);
        header.setRequestID(requestID);
        return header;
    }

    public static <T>T proxyGet(Class<T>  interfaceInfo){
        //实现各个版本的动态代理。。。。

        ClassLoader loader = interfaceInfo.getClassLoader();
        Class<?>[] methodInfo = {interfaceInfo};


        return (T) Proxy.newProxyInstance(loader, methodInfo, new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] methodArgs) throws Throwable {
                //如何设计我们的consumer对于provider的调用过程

                //1，调用 服务，方法，参数  ==》 封装成message  [content]
                String className = interfaceInfo.getName();
                String methodName = method.getName();
                Class<?>[] methodParameterTypes = method.getParameterTypes();
                MyContent content = new MyContent();

                content.setArgs(methodArgs);
                content.setName(className);
                content.setMethodName(methodName);
                content.setParameterTypes(methodParameterTypes);

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                ObjectOutputStream oout = new ObjectOutputStream(out);
                oout.writeObject(content);
                byte[] msgBody = out.toByteArray();

                //2，requestID+message  ，本地要缓存
                //协议：【header<>】【msgBody】
                Myheader header = createHeader(msgBody);

                out.reset();
                oout = new ObjectOutputStream(out);
                oout.writeObject(header);

                //解决数据decode问题
                //TODO：Server：： dispatcher  Executor
                byte[] msgHeader = out.toByteArray();

                System.out.println("main:::" + msgHeader.length);

                //3，连接池：：取得连接
                ClientFactory factory = ClientFactory.getFactory();
                NioSocketChannel clientChannel = factory.getClient(new InetSocketAddress("localhost", 9090));
                System.out.println("get clientChannel:"+clientChannel);

                //4. send -> IO, out->Netty
                ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.directBuffer(msgHeader.length + msgBody.length);

                CountDownLatch countDownLatch = new CountDownLatch(1);
                long id = header.getRequestID();
                ResponseHandler.addCallBack(id, new Runnable() {
                    @Override
                    public void run() {
                        countDownLatch.countDown();
                    }
                });

                byteBuf.writeBytes(msgHeader);
                byteBuf.writeBytes(msgBody);
                System.out.println("start to send request:" + msgHeader);
                ChannelFuture channelFuture = clientChannel.writeAndFlush(byteBuf);
                channelFuture.sync(); //blocking here

                // waiting for the response to come back
                countDownLatch.await();

                //how to resume code execution from here if the response come back in the future
                return null;

            }
        });
    }
}

class ResponseHandler {
    static ConcurrentHashMap<Long, Runnable> mapping = new ConcurrentHashMap<>();
    public static void addCallBack(long requestId, Runnable cb) {
        mapping.put(requestId, cb);
    }

    public static void runCallBack(long requestId) {
        Runnable cb = mapping.get(requestId);
        if (cb != null) {
            cb.run();
            removeCB(requestId);
        }
    }

    private static void removeCB(long requestId) {
        mapping.remove(requestId);
    }
}

// server handler
class ServerRequestHandler extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        Packmsg requestPkg = (Packmsg) msg;
        System.out.println("server handler: " + requestPkg.content.methodName + "server arg: " + requestPkg.content.getArgs()[0]);


//        ByteBuf sendBuf = buf.copy();
        /*
        while(buf.readableBytes() >= 85) {
            byte[] headerBytes = new byte[85];
            // 使用getBytes,指定从哪开始读（从读指针开始读），读多少(进哪里)，
            // ！！！ 不会真的移动读指针
            buf.getBytes(buf.readerIndex(), headerBytes);
//            buf.readBytes(headerBytes);
            ByteArrayInputStream bais = new ByteArrayInputStream(headerBytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            Myheader myheader = (Myheader) ois.readObject();
            System.out.println("data length from server Request handler: " + myheader.getDataLen());
            System.out.println("request ID from server Request handler: " + myheader.getRequestID());

            if(buf.readableBytes() >= myheader.dataLen) {
                //如果成功进入body段，移动读指针到header尾
                buf.readBytes(85);
                byte[] request = new byte[buf.readableBytes()];
                buf.readBytes(request);
                bais = new ByteArrayInputStream(request);
                ois = new ObjectInputStream(bais);
                MyContent content = (MyContent) ois.readObject();
                System.out.println("print class name from serverRequestHandler: " + content.getName()+"." + content.getMethodName());
            } else {
                System.out.println("channel else: " + buf.readableBytes());
            }
        }*/
        //request header 0x14141414
        // response header 0x14141424
        String ioThreadName = Thread.currentThread().getName();
        //1,直接在当前方法 处理IO和业务和返回

        //3，自己创建线程池
        //2,使用netty自己的eventloop来处理业务及返回

        //方法一
//        ctx.executor().execute(new Runnable() {
        //丢给别的executor来处理
        ctx.executor().parent().next().execute(new Runnable() {

            @Override
            public void run() {
                String execThreadName = Thread.currentThread().getName();
                MyContent content = new MyContent();
                String res = "io thread: " + ioThreadName + " exec thread: " + execThreadName + " from args " + requestPkg.content.getArgs();
                content.setRes(res);

                System.out.println(res);
                byte[] contentBytes = SerDerUtil.ser(content);

                Myheader resHeader = new Myheader();
                resHeader.setRequestID(requestPkg.header.getRequestID());
                resHeader.setFlag(0x14141424);
                resHeader.setDataLen(contentBytes.length);

                byte[] headBytes = SerDerUtil.ser(resHeader);
                PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
                ByteBuf byteBuf = allocator.directBuffer(headBytes.length+contentBytes.length);

                byteBuf.writeBytes(headBytes);
                byteBuf.writeBytes(contentBytes);
                ctx.writeAndFlush(byteBuf);

            }
        });

//        ChannelFuture channelFuture = ctx.writeAndFlush(buf);
//        channelFuture.sync();
    }
}

class ServerDecode extends ByteToMessageDecoder{

    // - 他的父类里有channelread 方法
    // 把你放在channelRead中的部分，现在可以放在decode函数中。该类会自动在channelRead前后
    // 为你实现留存和拼接。
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf buf, List<Object> out) throws Exception {
        while(buf.readableBytes() >= 110) {
            byte[] bytes = new byte[110];
            buf.getBytes(buf.readerIndex(),bytes);  //从哪里读取，读多少，但是readindex不变
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream oin = new ObjectInputStream(in);
            Myheader header = (Myheader) oin.readObject();


            //DECODE在2个方向都使用
            //通信的协议
            if(buf.readableBytes() >= header.getDataLen()){
                //处理指针
                buf.readBytes(85);  //移动指针到body开始的位置
                byte[] data = new byte[(int)header.getDataLen()];
                buf.readBytes(data);
                ByteArrayInputStream din = new ByteArrayInputStream(data);
                ObjectInputStream doin = new ObjectInputStream(din);

                if(header.getFlag() == 0x14141414){
                    MyContent content = (MyContent) doin.readObject();
                    out.add(new Packmsg(header,content));

                }else if(header.getFlag() == 0x14141424){
                    MyContent content = (MyContent) doin.readObject();
                    out.add(new Packmsg(header,content));
                }
            }else{
                break;
            }


        }
    }
}

class ClientResponses  extends ChannelInboundHandlerAdapter {
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            Packmsg response = (Packmsg) msg;
            //TODO
            ResponseHandler.runCallBack(response.header.getRequestID());

        }
    }
}

class Packmsg {

    Myheader header;
    MyContent content;

    public Myheader getHeader() {
        return header;
    }

    public void setHeader(Myheader header) {
        this.header = header;
    }

    public MyContent getContent() {
        return content;
    }

    public void setContent(MyContent content) {
        this.content = content;
    }

    public Packmsg(Myheader header, MyContent content) {
        this.header = header;
        this.content = content;
    }
}

class SerDerUtil {
    static ByteArrayOutputStream out = new ByteArrayOutputStream();

    public synchronized static byte[] ser(Object msg){
        out.reset();
        ObjectOutputStream oout = null;
        byte[] msgBody = null;
        try {
            oout = new ObjectOutputStream(out);
            oout.writeObject(msg);
            msgBody= out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return msgBody;


    }
}
