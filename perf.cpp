#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <random>
#include <vector>
#include "TimeSeriesStream.h"
#include "benchmark/benchmark.h"
#include <fstream>

using namespace std;
#define BENCHMARK_ARGS ->Arg(500000)  //->Arg(1000000)->Arg(200000000)

vector<long>* generateSequece(long from, long to) {
	auto data = new vector<long>(to - from);

	for (long i = 0; i < (to - from); i++) {
		if (i % 2) {
			(*data)[i] = from + i;
		} else {
			(*data)[i] = from + i - 1;
		}
	}

	return data;
}

std::vector<double>* generateSequeceRandomRepeat(double from, double to) {
	auto data = new std::vector<double>(to - from);

	double val = 0.1;
	data->push_back(val);
	for (long i = 1; i < (to - from); i++) {
		if (rand() % 2) {
			(*data)[i] = val * i;
		} else {
			(*data)[i] = (*data)[i - 1];
		}
	}

	return data;
}

std::vector<long>* generateRandom() {
	long from = 0;
	long to = 100000000;

	size_t count = 3 * 1000 * 1000;

	auto data = new std::vector<long>(count);

	// better rand
	std::random_device rd;
	std::mt19937 mt(rd());
	std::uniform_real_distribution<double> dist(from, to);

	for (size_t i = 0; i < count; i++) {
		data->push_back((long)dist(mt));
	}

	return data;
}

template <typename T>
static void benchmarkCompress(benchmark::State& state, std::vector<T>& data) {
	facebook::gorilla::TimeSeriesStream a;
	int compressedSize = 0;
	while (state.KeepRunning()) {
		for (size_t i = 0; i < data.size(); i++) {
			a.append(1000 + i, data[i], 0);
		}
		compressedSize = a.size();
		a.reset();
	}

	std::cout << "ratio: " << (double)(data.size() * sizeof(long)) / compressedSize << std::endl;
	state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(data.size() * sizeof(long)));
}

template <typename T>
static void benchmarkDecompress(benchmark::State& state, std::vector<T>& data) {
	facebook::gorilla::TimeSeriesStream a;

	for (size_t i = 0; i < data.size(); i++) {
		a.append(1000 + i, data[i], 0);
	}

	std::vector<double> out(data.size());
	folly::StringPiece compressed(a.getDataPtr(), a.size());

	while (state.KeepRunning()) {
		int n = a.readValues(out, compressed, data.size(), 0);
	}

	state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(data.size() * sizeof(long)));
}

static void BM_sequenceCompress(benchmark::State& state) {
	auto data = generateSequece(0, state.range(0));
	benchmarkCompress(state, *data);
	delete data;
}
BENCHMARK(BM_sequenceCompress) BENCHMARK_ARGS;

static void BM_sequenceDecompress(benchmark::State& state) {
	auto data = generateSequece(0, state.range(0));
	benchmarkDecompress(state, *data);
	delete data;
}
BENCHMARK(BM_sequenceDecompress) BENCHMARK_ARGS;

static void BM_RandRepeatCompress(benchmark::State& state) {
	auto data = generateSequeceRandomRepeat(0, state.range(0));
	benchmarkCompress(state, *data);
	delete data;
}
BENCHMARK(BM_RandRepeatCompress) BENCHMARK_ARGS;

static void BM_RandRepeatDecompress(benchmark::State& state) {
	auto data = generateSequeceRandomRepeat(0, state.range(0));
	benchmarkDecompress(state, *data);
	delete data;
}
BENCHMARK(BM_RandRepeatDecompress) BENCHMARK_ARGS;

static void BM_testRandomDistributionCompress(benchmark::State& state) {
	auto data = generateRandom();
	benchmarkCompress(state, *data);
	delete data;
}
BENCHMARK(BM_testRandomDistributionCompress) BENCHMARK_ARGS;

static void BM_testRandomDistributionDecompress(benchmark::State& state) {
	auto data = generateRandom();
	benchmarkDecompress(state, *data);
	delete data;
}
BENCHMARK(BM_testRandomDistributionDecompress) BENCHMARK_ARGS;

static std::vector<double>* readStockData(const char* path, bool isDouble, int scale) {
	std::ifstream infile(path);

	auto data = new std::vector<double>();

	std::string line;
	while (std::getline(infile, line)) {
		double val;
		if (isDouble) {
			val = std::stod(line);
			if (scale) {
				long v1 = scale * val;
				val = reinterpret_cast<double&>(v1);
			}
		} else {
			long v = std::stol(line);
			val = reinterpret_cast<double&>(v);
		}
		data->push_back(val);
	}

	return data;
}

#define MAKE_COMPRESSION_TEST(NAME, isDouble, file, scale)              \
	static void BM_fileDataCompression##NAME(benchmark::State& state) { \
		auto data = readStockData(file, isDouble, scale);               \
		benchmarkCompress(state, *data);                                \
		delete data;                                                    \
	}

MAKE_COMPRESSION_TEST(A, true, "data/ibm.data", 0)
BENCHMARK(BM_fileDataCompressionA);

MAKE_COMPRESSION_TEST(B, true, "data/ibm.data", 10000)
BENCHMARK(BM_fileDataCompressionB);

MAKE_COMPRESSION_TEST(C, false, "data/writes.data", 0)
BENCHMARK(BM_fileDataCompressionC);

#define MAKE_DECOMPRESSION_TEST(NAME, isDouble, file, scale)              \
	static void BM_fileDataDecompression##NAME(benchmark::State& state) { \
		auto data = readStockData(file, isDouble, scale);                 \
		benchmarkDecompress(state, *data);                                \
		delete data;                                                      \
	}

MAKE_DECOMPRESSION_TEST(A, true, "data/ibm.data", 0)
BENCHMARK(BM_fileDataDecompressionA);

MAKE_DECOMPRESSION_TEST(B, true, "data/ibm.data", 10000)
BENCHMARK(BM_fileDataDecompressionB);

MAKE_DECOMPRESSION_TEST(C, false, "data/writes.data", 0)
BENCHMARK(BM_fileDataDecompressionC);

BENCHMARK_MAIN();
