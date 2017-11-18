CC = cc

all:
	CC -o db sqlite.c
test:
	-rm ./db
	CC -o db sqlite.c
	rspec spec spec/test_spec.rb
clean:
	rm ./db