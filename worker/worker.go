//工人服务
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

type Task struct {
	Id    int `json:"id"`
	State int `json:"state"`
}

var masterAddress string
var storeageAddress string
var keyValueStoreAddress string

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Error: too few arguments .")
		return
	}

	keyValueStoreAddress = os.Args[1]

	resp, err := http.Get("http://" + keyValueStoreAddress + "/get?key=masterAddress")
	if err != nil {
		fmt.Println("Error: can't get master address.", err.Error())
		fmt.Println(resp.Body)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get master address.")
		fmt.Println(resp.Body)
		return
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	masterAddress = string(data)
	if len(masterAddress) == 0 {
		fmt.Println("Error:can't get master address. Length is zero.")
		return
	}

	//storeAddress
	resp, err = http.Get("http://" + keyValueStoreAddress + "/get?key=storeAddress")
	if err != nil {
		fmt.Println("Error: can't get store address.", err.Error())
		fmt.Println(resp.Body)
		return
	}

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error: can't get store address.")
		fmt.Println(resp.Body)
		return
	}

	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return
	}

	storeageAddress = string(data)
	if len(storeageAddress) == 0 {
		fmt.Println("Error:can't get store address. Length is zero.")
		return
	}

	//解析线程个数
	threadCount, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println("Error: can't parse thread count.")
		return
	}

	myWG := sync.WaitGroup{}

	myWG.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		//并发
		go func() {
			for {
				//do work
				//循环无止境的工作
				/*
				*		之间如果发生错误，则跳过档次循环，然后等待两秒钟
				*	one.	先获取任务
				*	two.	再从存储中获取图像数据
				* 	three.	再编辑图片
				*	four.	再将图片发送回存储
				* 	five.	最后注册完成的任务
				 */

				myTask, err := getNewTask(masterAddress)
				if err != nil {
					fmt.Println("Error:failure task", err)
					fmt.Println("Waiting 2 second timeout...")
					time.Sleep(2 * time.Second)
					continue
				}

				myImage, err := getImageFromStorage(storeageAddress, myTask)
				if err != nil {
					fmt.Println("Error:failure getImageFromStorage", err)
					fmt.Println("waiting 2 second timeout...")
					time.Sleep(2 * time.Second)
					continue
				}

				myImage = doWorkOnImage(myImage)

				err = sendImageToStorage(storeageAddress, myTask, myImage)
				if err != nil {
					fmt.Println("Error:failure sendImageToStorage", err)
					fmt.Println("waiting 2 second timeout...")
					time.Sleep(2 * time.Second)
					continue
				}
				err = registerFinishedTask(masterAddress, myTask)
				if err != nil {
					fmt.Println("Error:failure registerFinishedTask", err)
					fmt.Println("waiting 2 second timeout...")
					time.Sleep(2 * time.Second)
					continue
				}
			}
		}()
	}

	myWG.Wait()

}

//获取新任务
func getNewTask(masterAddress string) (Task, error) {
	resp, err := http.Post("http://"+masterAddress+"/getNewTask", "text/plain", nil)

	if err != nil || resp.StatusCode != http.StatusOK {
		return Task{-1, -1}, err
	}

	data, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return Task{-1, -1}, err
	}

	myTask := Task{}

	err = json.Unmarshal(data, &myTask)
	fmt.Println("json", err, myTask.Id, myTask.State, string(data))
	if err != nil {
		return Task{-1, -1}, err
	}
	fmt.Println("asdfasdf", myTask)
	return myTask, nil
}

//从存储服务获取图片
func getImageFromStorage(storeageAddress string, myTask Task) (image.Image, error) {
	resp, err := http.Get("http://" + storeageAddress + "/getImage?id=" + strconv.Itoa(myTask.Id) + "&state=working")

	if err != nil {
		return nil, err
	}

	myImage, err := png.Decode(resp.Body)
	if err != nil {
		return nil, err
	}
	return myImage, nil
}

//编辑图片
func doWorkOnImage(myImage image.Image) image.Image {
	myCanvas := image.NewRGBA(myImage.Bounds())
	for i := 0; i < myCanvas.Rect.Max.X; i++ {
		for j := 0; j < myCanvas.Rect.Max.Y; j++ {
			r, g, b, _ := myCanvas.At(i, j).RGBA()
			myColor := new(color.RGBA)
			myColor.R = uint8(r)
			myColor.G = uint8(g)
			myColor.B = uint8(b)
			myColor.A = uint8(255)
			myCanvas.Set(i, j, myColor)
		}
	}
	return myCanvas.SubImage(myImage.Bounds())
}

//发送图片到存储服务
func sendImageToStorage(storeageAddress string, myTask Task, myImage image.Image) error {
	data := []byte{}
	buffer := bytes.NewBuffer(data)
	err := png.Encode(buffer, myImage)
	if err != nil {
		return err
	}
	resp, err := http.Post("http://"+storeageAddress+"/sendImage?id="+strconv.Itoa(myTask.Id)+"&state=finished", "image/png", buffer)
	if err != nil || resp.StatusCode != http.StatusOK {
		return err
	}
	return nil

}

//注册已被完成的任务
func registerFinishedTask(masterAddress string, myTask Task) error {
	resp, err := http.Post("http://"+masterAddress+"/registerTaskFinished?id="+strconv.Itoa(myTask.Id)+"", "text/plain", nil)
	if err != nil || resp.StatusCode != http.StatusOK {
		return err
	}
	return nil
}
