package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private IVoucherOrderService proxy;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT; // 获取lua脚本

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    public void init() {
        // 该方法在当前类初始完成后执行
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.一人一单
        Long userId = voucherOrder.getUserId();

        synchronized (userId.toString().intern()) {
            // 5.1. 查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            // 5.2. 判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                log.error("用户已经购买过一次!");
                return;
            }


            //6.扣减库存
            //boolean success = seckillVoucherService.update()
            //        .setSql("stock = stock - 1") // set stock = stock-1
            //        .eq("voucher_id", voucherId).eq("stock", voucher.getStock()) // where id=? and stock=?
            //        .update();

            // 改进只要使得其值大于等于0即可 防止超卖
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock-1
                    .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) // where id=? and stock>0
                    .update();

            if (!success) {
                // 扣减失败
                log.error("库存不足!");
                return;
            }

            save(voucherOrder);
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1.获取用户
        Long userId = voucherOrder.getUserId();

        RLock lock = redissonClient.getLock("lock:order" + userId);
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if (!isLock) {
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }

        try {
            // 获取代理对象(事务)
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }


    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);
    //private class VoucherOrderHandler implements Runnable{
    //    @Override
    //    public void run() {
    //        while(true){
    //            try {
    //                //1.获取队列中的订单信息
    //                VoucherOrder voucherOrder = orderTasks.take();
    //                //2.创建订单
    //                handleVoucherOrder(voucherOrder);
    //            } catch (Exception e) {
    //                log.error("处理订单异常");
    //            }
    //        }
    //    }
    //}

    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    // 1.获取队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order >
                    // 这里为什么是list 因为count的值不一定是1 只是这里设置成了1
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed()) // 这里的读取标记值得是最近的没有被消费的消息>
                    );

                    // 2.判断消息获取是否 成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明没有消息， 继续下一次循环
                        continue;
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);

                    // 5.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    // 相当于消息没有被确认 消息就会进入pending list 接下来就需要到pending list中获取消息进行处理
                    log.error("处理订单异常");
                    handlePendinglist();
                }
            }
        }

        private void handlePendinglist() {
            while (true) {
                try {
                    // 1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.order 0
                    // 这里注意一下与上面那个写法的区别
                    // 相当于 >是从下一个未消费的消息开始 0是从pending-list中的第一个消息开始进行处理
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 2.判断消息获取是否 成功
                    if (list == null || list.isEmpty()) {
                        // 如果获取失败，说明 pending-list 里没有异常消息， 结束循环
                        break; // 上面的写法是continue
                    }

                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4. 如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常");

                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
        }
    }


    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 获取订单
        long orderId = redisIdWorker.nextId("order");

        // 1.执行Lua脚本 这段脚本执行完代表用户有购买资格 并且消息已经发出
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId)
        );

        // 2.判断结果是否为0
        int r = result.intValue();
        if (r != 0) { // 表示没有购买资格
            // 2.1. 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 3.返回订单id
        return Result.ok(orderId);
    }


    //@Override
    //public Result seckillVoucher(Long voucherId){
    //    // 获取用户
    //    Long userId = UserHolder.getUser().getId();
    //    // 1.执行Lua脚本
    //    Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
    //            Collections.emptyList(),
    //            voucherId.toString(),
    //            userId.toString()
    //    );
    //    // 2.判断结果是否为0
    //    int r = result.intValue();
    //    if(r!=0){ // 表示没有购买资格
    //        // 2.1. 不为0，代表没有购买资格
    //        return Result.fail(r==1?"库存不足":"不能重复下单");
    //    }
    //    // 2.2. 为0 有购买资格，把下单信息保存到阻塞队列
    //    long orderId = redisIdWorker.nextId("order");
    //    // TODO 保存阻塞队列
    //    VoucherOrder voucherOrder = new VoucherOrder();
    //    // 2.3.订单id
    //    orderId = redisIdWorker.nextId("order");
    //    voucherOrder.setId(orderId);
    //    // 2.4.用户id
    //    voucherOrder.setUserId(userId);
    //    // 2.5.代金券id
    //    voucherOrder.setVoucherId(voucherId);
    //    // 2.6.创建阻塞队列
    //    orderTasks.add(voucherOrder);
    //    // 3.获取代理对象
    //    proxy = (IVoucherOrderService) AopContext.currentProxy();
    //
    //
    //
    //    // 3.返回订单id
    //    return Result.ok(orderId);
    //}

    //@Override
    //public Result seckillVoucher(Long voucherId) {
    //    //1.查询优惠券
    //    SeckillVoucher voucher = seckillVoucherService.getById(voucherId); // 根据传入的优惠券id查询秒杀优惠券
    //    //2.判断秒杀是否开始
    //    if(voucher.getBeginTime().isAfter(LocalDateTime.now())){
    //        // 尚未开始
    //        return Result.fail("秒杀尚未开始!");
    //    }
    //    //3.判断秒杀是否已经结束
    //    if(voucher.getEndTime().isBefore(LocalDateTime.now())){
    //        // 尚未开始
    //        return Result.fail("秒杀已经结束!");
    //    }
    //    //4.判断库存是否重组
    //    if(voucher.getStock()<1){
    //        // 库存不足
    //        return Result.fail("库存不足!");
    //    }
    //
    //
    //    //// 先获取锁再进入函数 等函数执行完 等执行完 事务提交了 再释放锁 可以保证数据库是有订单的 等其他用户再来的时候就可以查询到该订单就不会出现重复下单的情况了
    //    //Long userId = UserHolder.getUser().getId();
    //    //
    //    //synchronized (userId.toString().intern()){
    //    //    // 获取代理对象(事务)
    //    //    IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    //    //    return proxy.createVoucherOrder(voucherId);
    //    //}
    //
    //    // 使用分布式锁的方式 实现一人一单
    //    // 创建锁对象
    //    Long userId = UserHolder.getUser().getId();
    //    //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
    //
    //    RLock lock = redissonClient.getLock("lock:order" + userId);
    //    boolean isLock = lock.tryLock();
    //    // 判断是否获取锁成功
    //    if(!isLock){
    //        // 获取锁失败，返回错误或重试
    //        return Result.fail("不允许重复下单");
    //    }
    //
    //    try {
    //        // 获取代理对象(事务)
    //        IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
    //        return proxy.createVoucherOrder(voucherId);
    //    } finally {
    //        // 释放锁
    //        lock.unlock();
    //    }
    //}
    //
}
