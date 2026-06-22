BUILD_DIR ?= build

.PHONY: all test asan tsan ubsan bench clean

all:
	cmake -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=RelWithDebInfo 2>/dev/null || true
	cmake --build $(BUILD_DIR) --parallel

test: all
	ctest --test-dir $(BUILD_DIR) --output-on-failure

asan:
	cmake -B build-asan -DCMAKE_BUILD_TYPE=Debug -DLIBCHAN_SANITIZE=address,undefined
	cmake --build build-asan --parallel
	ctest --test-dir build-asan --output-on-failure

tsan:
	cmake -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DLIBCHAN_SANITIZE=thread
	cmake --build build-tsan --parallel
	ctest --test-dir build-tsan --output-on-failure

ubsan:
	cmake -B build-ubsan -DCMAKE_BUILD_TYPE=Debug -DLIBCHAN_SANITIZE=undefined
	cmake --build build-ubsan --parallel
	ctest --test-dir build-ubsan --output-on-failure

bench:
	cmake -B build-bench -DCMAKE_BUILD_TYPE=Release -DLIBCHAN_BUILD_BENCH=ON
	cmake --build build-bench --parallel

clean:
	rm -rf build build-asan build-tsan build-ubsan build-bench
