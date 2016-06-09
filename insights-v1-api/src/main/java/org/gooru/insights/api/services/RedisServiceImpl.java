package org.gooru.insights.api.services;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

@Component
public class RedisServiceImpl implements RedisService {

	private static final StringRedisSerializer STRING_SERIALIZER = new StringRedisSerializer();

	private static final LongSerializer LONG_SERIALIZER = LongSerializer.INSTANCE;

	@Autowired(required = false)
	private final RedisTemplate<String, Long> redisLongTemplate = new RedisTemplate<>();

	@Autowired(required = false)
	private RedisTemplate<String, String> redisStringTemplate;

	@PostConstruct
	void init() {

		setStringSerializerTemplate();
		setLongSerializerTemplate();
	}

	private void setLongSerializerTemplate() {
		redisLongTemplate.setKeySerializer(STRING_SERIALIZER);
		redisLongTemplate.setValueSerializer(LONG_SERIALIZER);
	}

	private void setStringSerializerTemplate() {
		redisStringTemplate.setKeySerializer(STRING_SERIALIZER);
		redisStringTemplate.setValueSerializer(STRING_SERIALIZER);
	}

	private ValueOperations<String, Long> longOperation() {
		return redisLongTemplate.opsForValue();
	}

	private ValueOperations<String, String> stringOperation() {
		return redisStringTemplate.opsForValue();
	}

	public String getDirectValue(String key) {
		return stringOperation().get(key);
	}

	private enum LongSerializer implements RedisSerializer<Long> {

		INSTANCE;

		@Override
		public byte[] serialize(Long aLong) throws SerializationException {
			if (null != aLong) {
				return aLong.toString().getBytes();
			} else {
				return new byte[0];
			}
		}

		@Override
		public Long deserialize(byte[] bytes) throws SerializationException {
			if (bytes != null && bytes.length > 0) {
				return Long.parseLong(new String(bytes));
			} else {
				return null;
			}
		}
	}
}