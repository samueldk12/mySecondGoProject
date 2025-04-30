package main

import "fmt"

type Animal interface {
	Sound() string
}

type Dog struct {
}

func (Dog) Sound() string {
	return "Au! au!"
}

func whatDoesThisAnimalSay(a Animal) {
	fmt.Println(a.Sound())
}

func main(){
	dog := Dog{}
	whatDoesThisAnimalSay(dog)
}