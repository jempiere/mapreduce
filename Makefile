ifeq ($(OS),Windows_NT)
  uname_S := Windows
else
  uname_S := $(shell uname -s)
endif

all: run

build:
	go build

run: build
	ifeq ($(uname_S), Windows)
		mapreduce
	else
		./mapreduce
	endif
