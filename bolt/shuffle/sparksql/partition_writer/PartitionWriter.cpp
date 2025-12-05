#include "bolt/shuffle/sparksql/partition_writer/PartitionWriter.h"
#include <bolt/common/base/Exceptions.h>
#include "bolt/shuffle/sparksql/partition_writer/LocalPartitionWriter.h"
#include "bolt/shuffle/sparksql/partition_writer/rss/CelebornPartitionWriter.h"
using namespace bytedance::bolt::shuffle::sparksql;

std::unique_ptr<PartitionWriter> PartitionWriter::create(
    PartitionWriterOptions options,
    arrow::MemoryPool* pool) {
  if (options.partitionWriterType == PartitionWriterType::kLocal) {
    return std::make_unique<LocalPartitionWriter>(
        options.numPartitions,
        options,
        pool,
        options.dataFile,
        options.configuredDirs);
  } else if (options.partitionWriterType == PartitionWriterType::kCeleborn) {
    return std::make_unique<CelebornPartitionWriter>(
        options.numPartitions, options, pool, options.rssClient);
  } else {
    BOLT_FAIL(
        "Unsupported partition writer type: " + options.partitionWriterType);
  }
  return nullptr;
}