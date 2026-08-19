package main

import ( 
    "net/http" 
    _ "net/http/pprof" 
) 

var c = make(chan int) 

func main() { 
    for i := 0; i < 100; i++ {
        go f(0x10*i)
    }
    http.ListenAndServe("localhost:8080", nil) 
} 

func f(x int) { 
    g(x+1) 
} 

func g(x int) { 
    h(x+1) 
} 

func h(x int) { 
    c <- 1  // send a value to the channel c, which will block if no one is receiving from c
    f(x+1) 
}