all:
	gcc *.c -g -o server -levent
clean:
	rm server -rf
