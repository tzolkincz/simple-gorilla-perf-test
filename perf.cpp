#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <vector>
#include "TimeSeriesStream.h"
#include "benchmark/benchmark.h"
#include <fstream>

using namespace std;

vector<long> *generateSequece(long from, long to)
{
  auto data = new vector<long>(to - from);

  srand(time(0));
  for (long i = 0; i < (to - from); i++)
  {
    if (rand() % 2)
    {
      (*data)[i] = from + i;
    }
    else
    {
      (*data)[i] = from + i - 1;
    }
  }

  return data;
}

void testStockDataDecompression(benchmark::State &state)
{
  std::ifstream infile("ibm.data");

  std::vector<long> data;

  std::string line;
  while (std::getline(infile, line))
  {
    // scale to integer - with no decimal fraction
    long price10000x = 10000 * std::stod(line);
    data.push_back(price10000x);
  }

  facebook::gorilla::TimeSeriesStream a;

  for (size_t i = 0; i < data.size(); i++)
  {
    a.append(1000 + i, reinterpret_cast<double &>(data[i]), 0);
  }

  std::vector<double> out(data.size());

  folly::StringPiece compressed(a.getDataPtr(), a.size());

  while (state.KeepRunning())
  {
    int n = a.readValues(out, compressed, data.size(), 0);
  }

  state.SetBytesProcessed(state.iterations() * sizeof(long) * data.size());
}
BENCHMARK(testStockDataDecompression)->Arg(1);

void testStockDataCompression(benchmark::State &state)
{
  std::ifstream infile("ibm.data");

  std::vector<long> data;

  std::string line;
  while (std::getline(infile, line))
  {
    // scale to integer - with no decimal fraction
    long price10000x = 10000 * std::stod(line);
    data.push_back(price10000x);
  }

  facebook::gorilla::TimeSeriesStream a;

  int compressedLength = 0;
  while (state.KeepRunning())
  {
    for (size_t i = 0; i < data.size(); i++)
    {
      a.append(1000 + i, reinterpret_cast<double &>(data[i]), 0);
    }
    compressedLength = a.size();
    a.reset(); //just null positions not clear backend data (for perf test purposes)
  }

  // std::cout << "ratio1: " << (double)(data.size() * 8) / compressedLength << std::endl;

  state.SetBytesProcessed(state.iterations() * sizeof(long) * data.size());
}
BENCHMARK(testStockDataCompression)->Arg(1);

static void BM_scalarCompress(benchmark::State &state)
{
  facebook::gorilla::TimeSeriesStream a;

  auto data = generateSequece(0, state.range(0));
  char *compressed = (char *)malloc(state.range(0) * 8);

  while (state.KeepRunning())
  {
    for (size_t i = 0; i < state.range(0); i++)
    {
      bool b = a.append(1000 + i, (double)((*data)[i]), 0);
      if (!b)
      {
        cout << "no data compressed" << 10 + i << endl;
      }
    }
  }
  state.SetBytesProcessed(
      int64_t(state.iterations()) * int64_t(state.range(0) * sizeof(long)));
}
BENCHMARK(BM_scalarCompress)->Arg(64)->Arg(8 << 12)->Arg(8 << 14)->Arg(8 << 16);

static void BM_scalarDeCompress(benchmark::State &state)
{
  facebook::gorilla::TimeSeriesStream a;

  auto data = generateSequece(0, state.range(0));
  char *compressed = (char *)malloc(state.range(0) * 8);

  while (state.KeepRunning())
  {
    for (size_t i = 0; i < state.range(0); i++)
    {
      bool b = a.append(1000 + i, (double)((*data)[i]), 0);
      if (!b)
      {
        cout << "no data compressed" << 10 + i << endl;
      }
    }
  }
  state.SetBytesProcessed(
      int64_t(state.iterations()) * int64_t(state.range(0) * sizeof(long)));
}
BENCHMARK(BM_scalarDeCompress)->Arg(64)->Arg(8 << 12)->Arg(8 << 14)->Arg(8 << 16);

BENCHMARK_MAIN();