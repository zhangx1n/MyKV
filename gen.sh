protoDir="pb"
outDir="pb"
protoc -I ${protoDir} ${protoDir}/pb.proto \
  --go_out=${outDir} \
  --go-grpc_out=${outDir}