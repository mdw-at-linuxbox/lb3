all: lb3
URI=/home/mdw/t/unl.dest/usr/local
SDK=/home/mdw/t/sdk.dest/usr/local
LDFLAGS=-L$(SDK)/lib -Wl,-rpath,$(SDK)/lib -L$(URI)/lib 
CXXFLAGS=-I$(SDK)/include -I$(URI)/include -g
LIBS=-laws-cpp-sdk-s3 -laws-cpp-sdk-core -lnetwork-uri
lb3: lb3.o
	$(CXX) -o lb3 lb3.o $(LDFLAGS) $(LIBS) -lpthread
clean:
	rm -f lb3 lb3.o
