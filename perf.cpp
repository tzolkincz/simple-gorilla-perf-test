#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <vector>
#include "TimeSeriesStream.h"
#include "benchmark/benchmark.h"
#include <fstream>

using namespace std;

vector<long>* generateSequece(long from, long to) {
	auto data = new vector<long>(to - from);

	srand(time(0));
	for (long i = 0; i < (to - from); i++) {
		if (rand() % 2) {
			(*data)[i] = from + i;
		} else {
			(*data)[i] = from + i - 1;
		}
	}

	return data;
}

void testStockDataDecompression(benchmark::State& state) {
	std::ifstream infile("ibm.data");

	std::vector<long> data;

	std::string line;
	while (std::getline(infile, line)) {
		// scale to integer - with no decimal fraction
		long price10000x = 10000 * std::stod(line);
		data.push_back(price10000x);
	}

	facebook::gorilla::TimeSeriesStream a;

	for (size_t i = 0; i < data.size(); i++) {
		a.append(1000 + i, reinterpret_cast<double&>(data[i]), 0);
	}

	std::vector<double> out(data.size());

	folly::StringPiece compressed(a.getDataPtr(), a.size());

	while (state.KeepRunning()) {
		int n = a.readValues(out, compressed, data.size(), 0);
	}

	state.SetBytesProcessed(state.iterations() * sizeof(long) * data.size());
}
BENCHMARK(testStockDataDecompression)->Arg(1);

void testStockDataCompression(benchmark::State& state) {
	std::ifstream infile("data/ibm.data");

	std::vector<long> data;

	std::string line;
	while (std::getline(infile, line)) {
		// scale to integer - with no decimal fraction
		long price10000x = 10000 * std::stod(line);
		data.push_back(price10000x);
	}

	facebook::gorilla::TimeSeriesStream a;

	int compressedLength = 0;
	while (state.KeepRunning()) {
		for (size_t i = 0; i < data.size(); i++) {
			a.append(1000 + i, reinterpret_cast<double&>(data[i]), 0);
		}
		compressedLength = a.size();
		a.reset();  // just null positions not clear backend data (for perf test purposes)
	}

	std::cout << "ratio1: " << (double)(data.size() * 8) / compressedLength << std::endl;

	state.SetBytesProcessed(state.iterations() * sizeof(long) * data.size());
}
BENCHMARK(testStockDataCompression)->Arg(1);

#define MAKE_COMPRESSION_TEST(NAME, isDouble, file, scale)                                   \
	void genCompressionTest##NAME(benchmark::State& state) {                                 \
		std::ifstream infile(file);                                                          \
		std::vector<double> data;                                                            \
		std::string line;                                                                    \
		while (std::getline(infile, line)) {                                                 \
			double val;                                                                      \
			if (isDouble) {                                                                  \
				val = std::stod(line);                                                       \
				if (scale) {                                                                 \
					long v1 = scale * val;                                                   \
					val = reinterpret_cast<double&>(v1);                                     \
				}                                                                            \
			} else {                                                                         \
				long v = std::stol(line);                                                    \
				val = reinterpret_cast<double&>(v);                                          \
			}                                                                                \
			data.push_back(val);                                                             \
		}                                                                                    \
                                                                                             \
		facebook::gorilla::TimeSeriesStream a;                                               \
                                                                                             \
		int compressedLength = 0;                                                            \
		while (state.KeepRunning()) {                                                        \
			for (size_t i = 0; i < data.size(); i++) {                                       \
				a.append(1000 + i, data[i], 0);                                              \
			}                                                                                \
			compressedLength = a.size();                                                     \
			a.reset();                                                                       \
		}                                                                                    \
                                                                                             \
		std::cout << "ratio: " << (double)(data.size() * 8) / compressedLength << std::endl; \
                                                                                             \
		state.SetBytesProcessed(state.iterations() * sizeof(double) * data.size());          \
	}

MAKE_COMPRESSION_TEST(A, true, "data/ibm.data", 0)
BENCHMARK(genCompressionTestA);

MAKE_COMPRESSION_TEST(B, true, "data/ibm.data", 10000)
BENCHMARK(genCompressionTestB);

MAKE_COMPRESSION_TEST(C, true, "data/usages.data", 0)
BENCHMARK(genCompressionTestC);

MAKE_COMPRESSION_TEST(D, false, "data/writes.data", 0)
BENCHMARK(genCompressionTestD);

#define MAKE_DECOMPRESSION_TEST(NAME, isDouble, file, scale)                      \
	void genDecompressionTest##NAME(benchmark::State& state) {                    \
		std::ifstream infile(file);                                               \
		std::vector<double> data;                                                 \
		std::string line;                                                         \
		while (std::getline(infile, line)) {                                      \
			double val;                                                           \
			if (isDouble) {                                                       \
				val = std::stod(line);                                            \
				if (scale) {                                                      \
					long v1 = scale * val;                                        \
					val = reinterpret_cast<double&>(v1);                          \
				}                                                                 \
			} else {                                                              \
				long v = std::stol(line);                                         \
				val = reinterpret_cast<double&>(v);                               \
			}                                                                     \
			data.push_back(val);                                                  \
		}                                                                         \
                                                                                  \
		facebook::gorilla::TimeSeriesStream a;                                    \
                                                                                  \
		for (size_t i = 0; i < data.size(); i++) {                                \
			a.append(1000 + i, data[i], 0);                                       \
		}                                                                         \
                                                                                  \
		std::vector<double> out(data.size());                                     \
		folly::StringPiece compressed(a.getDataPtr(), a.size());                  \
                                                                                  \
		while (state.KeepRunning()) {                                             \
			int n = a.readValues(out, compressed, data.size(), 0);                \
		}                                                                         \
                                                                                  \
		state.SetBytesProcessed(state.iterations() * sizeof(long) * data.size()); \
	}

MAKE_DECOMPRESSION_TEST(A, true, "data/ibm.data", 0)
BENCHMARK(genDecompressionTestA);

MAKE_DECOMPRESSION_TEST(B, true, "data/ibm.data", 10000)
BENCHMARK(genDecompressionTestB);

MAKE_DECOMPRESSION_TEST(C, true, "data/usages.data", 0)
BENCHMARK(genDecompressionTestC);

MAKE_DECOMPRESSION_TEST(D, false, "data/writes.data", 0)
BENCHMARK(genDecompressionTestD);

static void BM_scalarCompress(benchmark::State& state) {
	facebook::gorilla::TimeSeriesStream a;

	auto data = generateSequece(0, state.range(0));
	char* compressed = (char*)malloc(state.range(0) * 8);

	while (state.KeepRunning()) {
		for (size_t i = 0; i < state.range(0); i++) {
			bool b = a.append(1000 + i, (double)((*data)[i]), 0);
			if (!b) {
				cout << "no data compressed" << 10 + i << endl;
			}
		}
	}
	state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0) * sizeof(long)));
}
BENCHMARK(BM_scalarCompress)->Arg(64)->Arg(8 << 12)->Arg(8 << 14)->Arg(8 << 16);

static void BM_scalarDeCompress(benchmark::State& state) {
	facebook::gorilla::TimeSeriesStream a;

	auto data = generateSequece(0, state.range(0));
	char* compressed = (char*)malloc(state.range(0) * 8);

	while (state.KeepRunning()) {
		for (size_t i = 0; i < state.range(0); i++) {
			bool b = a.append(1000 + i, (double)((*data)[i]), 0);
			if (!b) {
				cout << "no data compressed" << 10 + i << endl;
			}
		}
	}
	state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(state.range(0) * sizeof(long)));
}
BENCHMARK(BM_scalarDeCompress)->Arg(64)->Arg(8 << 12)->Arg(8 << 14)->Arg(8 << 16);

BENCHMARK_MAIN();