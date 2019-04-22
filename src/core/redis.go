package core

import (
	"github.com/go-redis/redis"
	"math"
	"strconv"
	"time"
)



func NewClientZero(config Config) (*redis.Client,error){
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisIP,
		Password: config.RedisPwd, // no password set
		DB:       0,  // use default DB
		DialTimeout:  180 * time.Second,
		ReadTimeout:  180 * time.Second,
		WriteTimeout: 180 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
	_,err := client.Ping().Result()
	if err!=nil{
		return nil,err
	}
	return client,nil
}

func NewClientOne(config Config) (*redis.Client,error){
	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisIP,
		Password: config.RedisPwd, // no password set
		DB:       1,  // use 1 DB
		DialTimeout:  180 * time.Second,
		ReadTimeout:  180 * time.Second,
		WriteTimeout: 180 * time.Second,
		PoolSize:     10,
		PoolTimeout:  30 * time.Second,
	})
	_,err := client.Ping().Result()
	if err!=nil{
		return nil,err
	}
	return client,nil
}

func NewClientOneByDefaultClient(client *redis.Client) (*redis.Client,error){
	_,err := client.Do("select","1").Result()
	if err!=nil{
		return nil,err
	}
	return client,nil
}


//批量插入
func PeUsersInsertRedis(client *redis.Client,numsStr []string) error{
	//先清除缓存
	f,err :=client.FlushDB().Result()
	if err!=nil{
		Logger("清除缓存旧数据出错："+err.Error())
		return err
	}
	Logger("清除缓存旧数据："+f)

	//分批入库
	nums := int(math.Ceil(float64(len(numsStr))/50000))
	for j:=0;j<nums;j++{
		phones := []string{}
		if j==nums-1{
			phones = numsStr[j*50000:]
		}else{
			phones = numsStr[j*50000:(j+1)*50000]
		}
		args := make([]interface{}, len(phones)*2, len(phones)*2+1)
		for index,numStr := range phones{
			in:=index*2
			args[in] = numStr
			args[in+1] = ""
		}
		re,err:=client.MSet(args...).Result()
		if err!=nil{
			return err
		}
		Logger("第"+strconv.Itoa(j)+"组数据插入redis："+re)
	}

	return nil
}

//批量插入-基础版
func PeUsersInsertRedisBasic(client *redis.Client,numsStr []string) error{
	args := make([]interface{}, len(numsStr)*2, len(numsStr)*2+1)
	for index,numStr := range numsStr{
		in:=index*2
		args[in] = numStr
		args[in+1] = ""
	}
	re,err:=client.MSet(args...).Result()
	if err!=nil{
		return err
	}
	Logger("insert into redis："+re)
	return nil
}