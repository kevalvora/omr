TARGET      := adjacencylist.bin
CPP         := g++
CC          := gcc
CFLAGS      := -w -MMD -std=c++11 
LIBS        := -lm -lpthread -ltcmalloc -lstdc++ 
LIBPATH     := ../../core

PROTOC      := protoc
PROTO_LIBS  := -lprotobuf 

RM          := rm -rf
OBJS        := data.pb.o $(LIBPATH)/util.o main.o

-include    $(OBJS:%.o=%.d)

CFLAGS += -O3

all: $(TARGET) cleanup

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OPTS) $^ $(LDLIBS) $(LIBS) $(PROTO_LIBS) $(INC) -o $@

%.pb.cc: %.proto
	$(PROTOC) -I=. --cpp_out=. $<

%.pb.o: %.pb.cc %.pb.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp %.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

clean:
	$(RM) $(LIBPATH)/*.o *.o $(LIBPATH)/*.d *.d $(TARGET) *.pb.*

cleanup:
	$(RM) $(LIBPATH)/*.o *.o $(LIBPATH)/*.d *.d *.pb.*
