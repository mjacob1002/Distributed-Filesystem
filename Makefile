PROTOC=protoc

.PHONY: proto clean

proto: proto/*.proto
	$(PROTOC) --go_out=proto/ proto/*.proto

clean: 
	rm -r pkg/gen_proto/*
	rm -r cmd/peer/cmd 
