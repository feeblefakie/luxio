LDFLAGS=-L/usr/local/lib -lluxio
CPPFLAGS=-O2 -I/usr/local/include

objs=put_test get_test
all: $(objs)

put_test: put_test.cpp
	g++ $< -o $@ $(CPPFLAGS) $(LDFLAGS) -lpthread

get_test: get_test.cpp
	g++ $< -o $@ $(CPPFLAGS) $(LDFLAGS) -lpthread

clean:
	rm -f $(objs) *.bidx

