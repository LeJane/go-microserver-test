//数据库服务
package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

/*
*		实现功能
				它会保存新任务并且指定连续的Id。
				x 它将允许获取新任务。
				x 它将允许通过Id获取任务。
				x 它将允许通过Id设置任务。
				x 状态将通过int来表示：
				x 0 – 未开始
				x 1 – 进行中
				x 2 – 已完成
*
*/

//全局存储任务
type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

var datastore map[int]Task
var datastoreMutex sync.RWMutex
var oldNotFinishedTask int //生产环境中要考虑int是否会溢出的可能性
var oNFTMutex sync.RWMutex

func main() {

	if !registerInkvStore() {
		return
	}

	datastore = make(map[int]Task)
	datastoreMutex = sync.RWMutex{}
	oldNotFinishedTask = 0
	oNFTMutex = sync.RWMutex{}

	http.HandleFunc("/getById", getById)
	http.HandleFunc("/newTask", newTask)
	http.HandleFunc("/getNewTask", getNewTask)
	http.HandleFunc("/setById", setById)
	http.HandleFunc("/finishTask", finishTask)
	http.HandleFunc("/list", list)
	http.ListenAndServe(":3001", nil)
}

//注册数据库
func registerInkvStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error:", "too few arguments.")
		return false
	}

	databaseAddress := os.Args[1] //数据库地址
	keyValueStoreAddress := os.Args[2]

	resp, err := http.Post("http://"+keyValueStoreAddress+"/set?key=databaseAddress&value="+databaseAddress, "", nil)

	if err != nil {
		fmt.Println(err)
		return false
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return false
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: Failure when contacting key-value store:", string(data))
		return false
	}
	return true
}

//通过id获取任务
func getById(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input")
			return
		}
		id, err := strconv.Atoi(values.Get("id"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}
		datastoreMutex.RLock()
		//很有可能产生索引越界问题
		bIsError := err != nil || id >= len(datastore)
		if bIsError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input")
			return
		}

		//读取
		datastoreMutex.RLock()
		value := datastore[id]
		datastoreMutex.RUnlock()

		resp, err := json.Marshal(value)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(resp))

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}

//新任务
func newTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		datastoreMutex.Lock()
		//直接创建任务
		TaskAdd := Task{
			Id:    len(datastore),
			State: 0,
		}
		datastore[TaskAdd.Id] = TaskAdd
		datastoreMutex.Unlock()
		fmt.Fprint(w, TaskAdd.Id)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}

}

//设置id
func setById(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		//		获取body
		taskToSet := Task{}

		data, err := ioutil.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		err = json.Unmarshal(data, &taskToSet)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		bIsError := false
		//判断id是否越界，以及状态是无效>2,表示无效
		datastoreMutex.Lock()
		if taskToSet.Id >= len(datastore) || taskToSet.State > 2 || taskToSet.State < 0 {
			bIsError = true
		} else {
			datastore[taskToSet.Id] = taskToSet
		}
		datastoreMutex.Unlock()

		if bIsError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Wrong input")
			return
		}

		fmt.Fprint(w, "success")

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}

//获取新任务
func getNewTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {

		bIsError := false

		datastoreMutex.RLock()
		if len(datastore) == 0 {
			bIsError = true
		}
		datastoreMutex.RUnlock()
		if bIsError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "No non-started task.")
			return
		}

		taskToSend := Task{Id: -1, State: 0}
		oNFTMutex.Lock()
		datastoreMutex.Lock()

		for i := oldNotFinishedTask; i < len(datastore); i++ {

			if datastore[i].State == 2 && i == oldNotFinishedTask {
				oldNotFinishedTask++
				continue
			}
			fmt.Println("iiii", i, datastore[i].State, datastore[i].Id)
			if datastore[i].State == 0 {
				datastore[i] = Task{Id: i, State: 1}
				taskToSend = datastore[i]
				break
			}
		}
		datastoreMutex.Unlock()

		oNFTMutex.Unlock()

		if taskToSend.Id == -1 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "No non-started task.")
			return
		}

		myId := taskToSend.Id

		go func() {
			time.Sleep(time.Second * 120)
			datastoreMutex.Lock()
			if datastore[myId].State == 1 {
				datastore[myId] = Task{Id: myId, State: 0}
			}
			datastoreMutex.Unlock()
		}()

		resp, err := json.Marshal(taskToSend)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		fmt.Fprint(w, string(resp))

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}

//完成任务
func finishTask(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		//		获取值
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input")
			return
		}

		id, err := strconv.Atoi(values.Get("id"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, err)
			return
		}

		updateTask := Task{Id: id, State: 2}
		bIsError := false

		datastoreMutex.Lock()
		if datastore[id].State == 1 {
			datastore[id] = updateTask
		} else {
			bIsError = true
		}
		datastoreMutex.Unlock()

		if bIsError {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input")
			return
		}

		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}

//列表
func list(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		datastoreMutex.RLock()
		for key, value := range datastore {
			fmt.Fprintln(w, key, ":", "id:", value.Id, "state:", value.State)
		}
		datastoreMutex.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}
