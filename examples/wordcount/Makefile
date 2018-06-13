TARGET      := wordcount.bin
TARGET_ONE  := wordcount-one.bin
CPP         := g++
CC          := gcc
CFLAGS      := -w -MMD -std=c++11 -O3
LIBS        := -lm -lpthread -ltcmalloc -lstdc++ 
LIBPATH     := ../../core

PROTOC      := protoc
PROTO_LIBS  := -lprotobuf 

RM          := rm -rf
OBJS        := data.pb.o $(LIBPATH)/util.o main.o
OBJS_ONE    := $(LIBPATH)/util.o main-one.o

-include    $(OBJS:%.o=%.d)

all: $(TARGET) $(TARGET_ONE) cleanup

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $(OPTS) $^ $(LDLIBS) $(LIBS) $(PROTO_LIBS) $(INC) -o $@

$(TARGET_ONE): $(OBJS_ONE)
	$(CC) $(CFLAGS) $(OPTS) $^ $(LDLIBS) $(LIBS) $(PROTO_LIBS) $(INC) -DUSE_ONE_PHASE_IO -o $@

%.pb.cc: %.proto
	$(PROTOC) -I=. --cpp_out=. $<

%.pb.o: %.pb.cc %.pb.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp %.h
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

%.o: %.cpp
	$(CC) $(CFLAGS) $(OPTS) -c $< -o $@

main-one.o: main.cpp
	$(CC) $(CFLAGS) $(OPTS) -c $< -DUSE_ONE_PHASE_IO -o $@

clean:
	$(RM) $(LIBPATH)/*.o *.o $(LIBPATH)/*.d *.d $(TARGET) $(TARGET_ONE) *.pb.*

cleanup:
	$(RM) $(LIBPATH)/*.o *.o $(LIBPATH)/*.d *.d *.pb.*
