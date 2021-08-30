# Makefile for sample programs of Tkrzw

#================================================================
# Setting Variables
#================================================================

# Generic settings
SHELL = /bin/sh

# Targets
MYBINS = helloworld \
  remotedbm_ex1 remotedbm_ex2

# Building binaries
CXX = g++
CPPFLAGS = -I. -I.. -I/usr/local/include -D_GNU_SOURCE=1
CXXFLAGS = -std=c++17 -Wall -fsigned-char -O2
LDFLAGS = -L. -L.. -L/usr/local/lib 
LIBS = -ltkrzw_rpc -lgrpc++_reflection -lgrpc++ -lgrpc -lprotobuf \
  -ltkrzw -ltkrzw -lstdc++ -lpthread
LDENV = LD_RUN_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..
RUNENV = LD_LIBRARY_PATH=/lib:/usr/lib:$(HOME)/lib:/usr/local/lib:.:..

#================================================================
# Suffix rules
#================================================================

.SUFFIXES :
.SUFFIXES : .cc .o

.cc.o :
	$(CXX) -c $(CPPFLAGS) $(CXXFLAGS) $<

#================================================================
# Actions
#================================================================

all : $(MYBINS)

clean :
	rm -rf $(MYBINS) *.exe *.o a.out check.out gmon.out leak.log casket* *.tkh *.tkt *.tks *~

static :
	make LDFLAGS="$(LDFLAGS) -static"

single :
	g++ -std=c++17 -I/usr/local/include -Wall -fsigned-char -O2 \
	  -o helloworld helloworld.cc -L/usr/local/lib \
	  -lgrpc++_reflection -lgrpc++ -lgrpc -lprotobuf -ltkrzw -lstdc++ -lpthread

.PHONY : all clean static single

#================================================================
# Building binaries
#================================================================

helloworld : helloworld.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

remotedbm_ex1 : remotedbm_ex1.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

remotedbm_ex2 : remotedbm_ex2.o
	$(LDENV) $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $< $(LDFLAGS) $(LIBS)

# END OF FILE
