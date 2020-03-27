import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * 分布式锁
 */
@Component
@Slf4j
public class DistributeLock {

	@Resource
	private RedisCacheClient jedisCluster;

	private static final String SET_IF_NOT_EXIST = "NX";
	//毫秒
	private static final String SET_WITH_EXPIRE_TIME = "PX";
	private static final String LOCK_SUCCESS = "OK";

	private boolean tryLock(String key, String requestId, int expire) {
		String result = jedisCluster.getJedis().set(key, requestId, SET_IF_NOT_EXIST, SET_WITH_EXPIRE_TIME, expire);
		return LOCK_SUCCESS.equals(result);
	}

	private void unlock(String key, String requestId) {
		String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		jedisCluster.getJedis().eval(script, Collections.singletonList(key), Collections.singletonList(requestId));
	}


	/**
	 * 用分布式锁保证并发事务的执行, 多个并发执行最终只会执行一个, 多用于定时任务的集群并发场景
	 *
	 * @param lockKey                锁的key, 建议按业务做区分
	 * @param callable               需要执行的事务
	 * @param releaseLockImmediately 是否在任务执行完毕后立即释放锁
	 * @param requestId              标识锁的拥有者才能释放锁
	 * @param expire                 过期的时间 毫秒
	 */
	public <T> T call(String lockKey, Callable<T> callable, boolean releaseLockImmediately, String requestId, int expire) {
		boolean fetchLock = false;

		try {

			fetchLock = tryLock(lockKey, requestId, expire);

			if (fetchLock) {
				return callable.call();
			} else {
				log.info("can not lock, lockKey:[{}]", lockKey);
			}

		} catch (Exception e) {
			log.error("distribute lock fail", e);
		} finally {

			//获取到锁的线程才允许释放锁
			if (releaseLockImmediately && fetchLock) {
				unlock(lockKey, requestId);
			}
		}

		return null;
	}


	/**
	 * 阻塞式 用分布式锁保证并发事务的执行, 多个并发执行最终只会执行一个, 为了保证所有的事务都执行完，拿不到锁就进行等待和重试
	 *
	 * @param lockKey                锁的具体名称
	 * @param callable               具体执行的内容
	 * @param releaseLockImmediately 是否立即解锁
	 * @param retryTime              重试的次数
	 * @param sleepTime              阻塞的时间 单位：毫秒
	 * @param requestId              标识锁的拥有者才能释放锁
	 * @param expire                 过期的时间
	 * @param <T>
	 * @return
	 */
	public <T> T callWithRetry(String lockKey, Callable<T> callable, boolean releaseLockImmediately, Integer retryTime,
			Integer sleepTime, String requestId, int expire) {
		boolean lock = false;

		while (retryTime > 0) {
			try {

				lock = tryLock(lockKey, requestId, expire);
				if (lock) {

					try {
						// return之后就执行finally不会再进入while循环了
						return callable.call();
					} catch (Exception e) {
						log.error("distribute transaction call fail", e);
						// 出现异常就不进行重试
						break;
					}

				} else {
					// 拿不到锁才会等待
					Thread.sleep(sleepTime);
					log.info("can not withRetryLock, lockKey:[{}] and retryTime is [{}]", lockKey, retryTime);
					retryTime--;
				}

			} catch (Exception e) {
				log.error("distribute lock fail", e);
			} finally {
				//获取到锁的线程才允许释放锁
				if (releaseLockImmediately && lock) {
					unlock(lockKey, requestId);
				}
			}
		}
		return null;
	}


	/**
	 * 用分布式锁保证并发事务的执行, 多个并发执行最终只会执行一个, 多用于定时任务的集群并发场景
	 *
	 * @param lockKey                锁的key, 建议按业务做区分
	 * @param runnable               需要执行的事务
	 * @param releaseLockImmediately 是否在任务执行完毕后立即释放锁
	 * @param requestId              标识锁的拥有者才能释放锁
	 * @param expire                 过期的时间 毫秒
	 */
	public void execute(String lockKey, Runnable runnable, boolean releaseLockImmediately, int expire, String requestId) {
		boolean fetchLock = false;

		try {

			fetchLock = tryLock(lockKey, requestId, expire);

			if (fetchLock) {
				runnable.run();
			} else {
				log.info("can not lock, lockKey:[{}]", lockKey);
			}

		} catch (Exception e) {
			log.error("distribute lock fail", e);
		} finally {

			//获取到锁的线程才允许释放锁
			if (releaseLockImmediately && fetchLock) {
				unlock(lockKey, requestId);
			}
		}
	}


}
