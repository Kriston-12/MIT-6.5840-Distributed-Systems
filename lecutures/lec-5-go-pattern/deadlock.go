package main

import "fmt"

func Schedule(
    servers chan string,
    numTask int,
    call func(srv string, task int),
) {
    // 两个都是无缓冲 channel
    work := make(chan int)
    done := make(chan bool)

    runTasks := func(srv string) {
        for task := range work {
            call(srv, task)

            // 通知 main：一个 task 完成了
            done <- true
        }
    }

    // 动态启动 server worker
    go func() {
        for srv := range servers {
            go runTasks(srv)
        }
    }()

    // 先发送所有任务
    for task := 0; task < numTask; task++ {
        work <- task
    }

    close(work)

    // 然后才接收完成通知
    for i := 0; i < numTask; i++ {
        <-done
    }
}

func main() {
    servers := make(chan string)

    go func() {
        servers <- "S1"
        close(servers)
    }()

    Schedule(servers, 2, func(srv string, task int) {
        fmt.Printf("%s finished task %d\n", srv, task)
    })
}