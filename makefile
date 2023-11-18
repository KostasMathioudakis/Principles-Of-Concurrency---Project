all:
	gcc -pthread phaseA.c -o phaseA
clean:
	rm -rf ./phaseA
