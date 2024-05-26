use std::collections::HashMap;

use redis::{aio::MultiplexedConnection, AsyncCommands, Client, FromRedisValue, RedisResult};

#[derive(Debug, Clone)]
pub struct RedisClient {
    pub db_name: String,
    pub client: Client,
}

impl RedisClient {
    pub fn connect(url: &str, db_name: &str) -> RedisResult<Self> {
        let client = Client::open(url)?;
        Ok(Self {
            client,
            db_name: db_name.to_string(),
        })
    }

    pub async fn get_con(&self) -> RedisResult<MultiplexedConnection> {
        self.client.get_multiplexed_async_connection().await
    }

    pub async fn set_str(&self, key: &str, value: &str, ttl_seconds: i64) -> RedisResult<()> {
        let mut con = self.get_con().await?;
        con.set(key, value).await?;
        if ttl_seconds > 0 {
            con.expire(key, ttl_seconds).await?;
        }
        Ok(())
    }

    pub async fn hset(&self, key: &str, field: &str, value: &str) -> RedisResult<()> {
        let mut con = self.get_con().await?;
        con.hset(key, field, value).await
    }

    pub async fn hget(&self, key: &str, field: &str) -> RedisResult<String> {
        let mut con = self.get_con().await?;
        con.hget(key, field).await
    }

    pub async fn hgetall(&self, key: &str) -> RedisResult<HashMap<String, String>> {
        let mut con = self.get_con().await?;
        let val = con.hgetall(key).await?;
        FromRedisValue::from_redis_value(&val)
    }

    pub async fn hdel(&self, key: &str, field: &str) -> RedisResult<()> {
        let mut con = self.get_con().await?;
        con.hdel(key, field).await
    }

    pub async fn get_str(&self, key: &str) -> RedisResult<String> {
        let mut con = self.get_con().await?;
        let value = con.get(key).await?;
        FromRedisValue::from_redis_value(&value)
    }
}
