//主要服务
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
)

var databaseLocation string
var storageAddress string

//全局存储任务
type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

func main() {

	if !registerInkvStore() {
		return
	}

	//要想知道数据库的地址和存储地址的，就必须知道键值储存地址
	keyValueStoreAddress := os.Args[2]

	resp, err := http.Get("http://" + keyValueStoreAddress + "/get?key=databaseAddress")
	if err != nil {
		fmt.Println("can't get databaseAddress.")
		fmt.Println(resp.Body)
		return
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	databaseLocation = string(data)

	//store
	resp, err = http.Get("http://" + keyValueStoreAddress + "/get?key=storeAddress")
	if err != nil {
		fmt.Println("can't get storageAddress.")
		fmt.Println(resp.Body)
		return
	}

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}
	storageAddress = string(data)

	http.HandleFunc("/new", newImage)
	http.HandleFunc("/get", getImage)
	http.HandleFunc("/getNewTask", getNewTask)
	http.HandleFunc("/isReady", isReady)
	http.HandleFunc("/registerTaskFinished", registerTaskFinished)
	http.ListenAndServe(":3003", nil)
}

//新图片
func newImage(w http.ResponseWriter, r *http.Request) {
	//首先通过数据库的newTask方法获取任务id，然后再通过id将图片写入存储服务里面
	if r.Method == http.MethodPost {
		resp, err := http.Post("http://"+databaseLocation+"/newTask", "text/plain", nil)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		id, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}
		_, err = http.Post("http://"+storageAddress+"/sendImage?id="+string(id)+"&state=working", "image", r.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}
		fmt.Fprint(w, string(id))
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:Not support method.")
	}
}

//获取新图片
func getImage(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input id.")
			return
		}

		resp, err := http.Get("http://" + storageAddress + "/getImage?id=" + values.Get("id") + "&state=finished")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:Not support method.")
	}
}

//是否准备好
func isReady(w http.ResponseWriter, r *http.Request) {
	//首先从url中获取id,然后通过id获取任务，解析到Task结构里面，然后判断状态
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:Wrong input id.")
			return
		}

		resp, err := http.Get("http://" + databaseLocation + "/getById?id=" + values.Get("id") + "")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		myTask := Task{}

		json.Unmarshal(data, &myTask)

		if myTask.State == 2 {
			fmt.Fprint(w, "1")
		} else {
			fmt.Fprint(w, "0")
		}

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:Not support method.")
	}
}

//获取新任务
func getNewTask(w http.ResponseWriter, r *http.Request) {
	//首先
	if r.Method == http.MethodPost {
		resp, err := http.Post("http://"+databaseLocation+"/getNewTask", "text/plain", nil)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:Not support method.")
	}
}

//注册被完成的任务
func registerTaskFinished(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

		if len(values.Get("id")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: Wrong input id.")
			return
		}

		resp, err := http.Post("http://"+databaseLocation+"/finishTask?id="+values.Get("id")+"", "text/plain", nil)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error: ", err)
			return
		}

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:Not support method.")
	}
}

//将masterAddress注册进入键值存储服务里面
func registerInkvStore() bool {
	if len(os.Args) < 3 {
		fmt.Println("Error:", "too few arguments.")
		return false
	}

	masterAddress := os.Args[1]
	keyValueStoreAddress := os.Args[2]

	resp, err := http.Post("http://"+keyValueStoreAddress+"/set?key=masterAddress&value="+masterAddress+"", "", nil)
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error:", err)
		return false
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: failure when contacting key-value store ", string(data))
		return false
	}
	return true
}
