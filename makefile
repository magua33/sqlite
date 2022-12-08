run:
	go run main.go statement.go input.go tb_*.go mydb.db

test:
	go test -v . -test.run Insert

bench:
	go test -bench='WriteBy' -test.bench WriteBy -test.run WriteBy -benchmem