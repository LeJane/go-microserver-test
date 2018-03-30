//微服务的键值存储和数据库的实现
package main

/*
*			实现功能
				. 键值存储
				. 数据库
*			实现思路
				.把键值对作为全局映射来存取，并且为并发存取设置一个全局读写锁
				主要实现方法
						.	设置-set
						.	获取-get
						.	删除-del
						.	列表-list
*/

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
)

//创建全局存储
var kvStoreMap map[string]string

//全局锁
var kvStoreSync sync.RWMutex

func main() {
	kvStoreMap = make(map[string]string)
	kvStoreSync = sync.RWMutex{}
	//注册url
	http.HandleFunc("/set", set)
	http.HandleFunc("/get", get)
	http.HandleFunc("/remove", remove)
	http.HandleFunc("/list", list)
	http.ListenAndServe(":3000", nil)
}

//设置
func set(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}
		//检查key
		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input key.")
		}

		if len(values.Get("value")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input value.")
			return
		}

		//写入全局map
		kvStoreSync.Lock()
		kvStoreMap[values.Get("key")] = values.Get("value")
		kvStoreSync.Unlock()
		fmt.Fprint(w, "success")
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method")
		return
	}
}

//获取
func get(w http.ResponseWriter, r *http.Request) {
	//判断方法
	if r.Method == http.MethodGet {
		//判断url是否正确
		values, err := url.ParseQuery(r.URL.RawQuery)

		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input key.")
			return
		}

		kvStoreSync.RLock()
		value := kvStoreMap[values.Get("key")]
		kvStoreSync.RUnlock()
		fmt.Fprint(w, value)
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
		return
	}
}

//删除
func remove(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodDelete {
		//提交key
		values, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", err)
			return
		}

		if len(values.Get("key")) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Error:", "Wrong input key.")
			return
		}

		//删除
		kvStoreSync.Lock()
		delete(kvStoreMap, values.Get("key"))
		kvStoreSync.Unlock()
		fmt.Fprint(w, "success")

	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
	}
}

//列表
func list(w http.ResponseWriter, r *http.Request) {
	//get
	if r.Method == http.MethodGet {
		kvStoreSync.RLock()
		for key, value := range kvStoreMap {
			fmt.Fprint(w, key, ":", value)
		}
		kvStoreSync.RUnlock()
	} else {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "Error:", "Not support method.")
		return
	}
}
