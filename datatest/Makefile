prog=data_append_test
all: $(prog)

data_append_test: data_append_test.cpp ../data.h
	g++ -g $< -o $@

check:
	./data_append_test

clean:
	rm -f $(prog) datadb
