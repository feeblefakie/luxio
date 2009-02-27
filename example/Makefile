LDFLAGS=-L/usr/local/lib
CPPFLAGS=-O2

objs=put_test get_test
all: $(objs)

put_test: put_test.cpp
	g++ $< -o $@ $(CPPFLAGS) $(LDFLAGS) -lpthread

get_test: get_test.cpp
	g++ $< -o $@ $(CPPFLAGS) $(LDFLAGS) -lpthread

clean:
	rm -f $(objs) *.bidx

