package core

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
)

type Job interface {
	Do() error
}

var JobQueue =  make(chan Job)

//type JobChannelType chan Job

type Worker struct {
	//多条流水线组成工作池
	WorkerPool chan chan Job
	//多个工作单位（需加工产品）组成一条流水线
	JobChannel chan Job
	quit chan bool
}
func NewWorker(workerPool chan chan Job) Worker{
	return Worker{
		workerPool,
		make(chan Job),
		make(chan bool),
	}
}
func (w Worker)Start(){
	go func() {
		for {
			// 将工人当前的流水线注册到总的工作池，register current worker into the worker queue
			// 注意 <- 这个是将式子右边的放入左边储存， 而 := <-  是将右边储存的数据拿出来赋值给左边的变量
			w.WorkerPool <- w.JobChannel
			select {
			case job := <- w.JobChannel:
				//we have received a work request
				if err:=job.Do();err!=nil{
					fmt.Printf("execute job failed with err: %v",err)
				}
				// receive quit event, stop worker
			case <- w.quit:
				return
			}
		}
	}()
}

func (w Worker)Stop(){
	go func() {
		w.quit<-true
	}()
}

//作业者池
//dispatcher := NewDispatcher(MaxWorker)
//dispatcher.Run()
type Dispatcher struct {
	//A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
}

func NewDispatcher(maxWorkers int)*Dispatcher{
	pool:= make(chan chan Job,maxWorkers)
	return &Dispatcher{pool}
}

func (d *Dispatcher) dispatch(){
	for{
		select {
		case job := <- JobQueue:
			//a job request has been received
			go func(job Job){
				//工作池分配一条流水线
				jobChannel := <- d.WorkerPool
				//最新的要加工产品进入所分配流水线
				jobChannel <- job
			}(job)
		}
	}
}

func (d *Dispatcher) Run(concurrency int){
	for i:=0;i< concurrency;i++{
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
	go d.dispatch()
}

type Jobber struct {
	test string
}

func (j *Jobber)Do() error{
	Logger("rand num: "+j.test)
	return nil
}

func Balance(){
	queue := make(chan Job)
	//并发数
	concurrency := 100
	JobQueue = queue
	dispatcher := NewDispatcher(concurrency)
	dispatcher.Run(concurrency)

	for i:=0;i<100;i++{
		jobber:=&Jobber{}
		jobber.test = strconv.Itoa(rand.Intn(100))
		JobQueue <- jobber
	}
}

type NetJobber struct {
	W http.ResponseWriter
	Req *http.Request
	Body []byte //req.body在开启新协程后会被主动关闭，所以要先记录
}

func (job *NetJobber) Do()error{
	//读取配置文件
	configPath := "./config.conf"
	config,err := ReadConfig(configPath)
	if err!=nil{
		Logger(err.Error())
		return err
	}
	auth := job.Req.Header.Get("Authorization")
	if auth == "" {
		job.W.Header().Set("WWW-Authenticate", `Basic realm="Unauthorized"`)
		job.W.WriteHeader(http.StatusUnauthorized)
		Logger("error auth")
		return nil
	}
	auths := strings.SplitN(auth," ",2)
	if len(auths)!=2{
		job.W.Write([]byte("Error Arguments"))
		Logger("error Arguments")
		return nil
	}
	authMethod:=auths[0]
	authB64 := auths[1]
	switch authMethod {
	case "Basic":
		authstr, err := base64.StdEncoding.DecodeString(authB64)
		if err != nil {
			Logger(err.Error())
			job.W.Write([]byte("Unauthorized!"))
			return nil
		}
		userPwd := strings.SplitN(string(authstr), "@", 2)
		if len(userPwd) != 2 {
			Logger("Error authB64")
			job.W.Write([]byte("Unauthorized!"))
			return nil
		}

		if err != nil {
			Logger(err.Error())
			job.W.Write([]byte("500"))
			return err
		}
		phone := &Phone{}
		if err != nil {
			Logger(err.Error())
		}
		err = json.Unmarshal(job.Body,phone)
		if err != nil {
			Logger(err.Error())
		}
		if len(phone.Mobile)!=11{
			Logger("Error phone number")
			job.W.Write([]byte("Error phone number"))
			return nil
		}
		bool,err:= CheckIsNewUser(phone.Mobile,config)
		if err != nil {
			Logger(err.Error())
			job.W.Write([]byte("500"))
			return nil
		}
		jsonObj := make(map[string]interface{})
		jsonObj["Mobile"] = phone.Mobile
		jsonObj["IsPeNewUser"] = bool
		bytesData, err := json.Marshal(jsonObj)
		if err != nil {
			Logger(err.Error())
			job.W.Write([]byte("500"))
			return nil
		}
		job.W.Write(bytesData)
	default:
		Logger("Not Basic Method")
		job.W.Write([]byte("Not Basic Method"))
	}
	return nil
}

//网络并发
func NetBalance(concurrency int) error{
	dispatcher := NewDispatcher(concurrency)
	dispatcher.Run(concurrency)
	return nil
}

