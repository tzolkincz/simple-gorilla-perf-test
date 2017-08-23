#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <vector>
#include "TimeSeriesStream.h"
#include "benchmark/benchmark.h"

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
  state.SetBytesProcessed(
      int64_t(state.iterations()) * int64_t(state.range(0) * sizeof(long)));
}
BENCHMARK(BM_scalarCompress)->Arg(64)->Arg(8 << 12)->Arg(8 << 14)->Arg(8 << 16);

BENCHMARK_MAIN();