include $(GOROOT)/src/Make.$(GOARCH)

TARG=mongo
GOFILES=\
	bson.go\

include $(GOROOT)/src/Make.pkg