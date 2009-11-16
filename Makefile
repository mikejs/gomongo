include $(GOROOT)/src/Make.$(GOARCH)

TARG=mongo
GOFILES=\
	bson.go\
	bson_struct.go\
	mongo.go\

include $(GOROOT)/src/Make.pkg