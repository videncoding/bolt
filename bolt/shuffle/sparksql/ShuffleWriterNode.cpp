#include "bolt/shuffle/sparksql/ShuffleWriterNode.h"
#include "bolt/common/memory/sparksql/ExecutionMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltArrowMemoryPool.h"
#include "bolt/shuffle/sparksql/BoltRowBasedSortShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriter.h"
#include "bolt/shuffle/sparksql/BoltShuffleWriterV2.h"
using namespace bytedance::bolt::shuffle::sparksql;
using namespace bytedance::bolt;
using namespace bytedance::bolt::exec;
using namespace bytedance::bolt::memory::sparksql;

SparkShuffleWriter::SparkShuffleWriter(
    int32_t operatorId,
    bytedance::bolt::exec::DriverCtx* driverCtx,
    std::shared_ptr<const SparkShuffleWriterNode> shuffleWriterNode)
    : bytedance::bolt::exec::Operator(
          driverCtx,
          shuffleWriterNode->outputType(),
          operatorId,
          shuffleWriterNode->id(),
          std::string(shuffleWriterNode->name())),
      shuffleWriterOptions_(shuffleWriterNode->getShuffleWriterOptions()),
      reportShuffleStatusCallback_(
          shuffleWriterNode->getReportShuffleStatusCallback()) {}

void SparkShuffleWriter::init(const bytedance::bolt::RowVectorPtr& rv) {
  arrowPool_ = std::make_unique<BoltArrowMemoryPool>(pool());
  auto freeMem = ExecutionMemoryPool::getMinimumFreeMemoryForTask(
      shuffleWriterOptions_.taskAttemptId);
  BOLT_CHECK(
      freeMem.has_value(),
      "Expect ExecutionMemoryPool::getMinimumFreeMemoryForTask return value");
  shuffleWriter_ = BoltShuffleWriter::create(
      shuffleWriterOptions_,
      rv->childrenSize() - 1,
      rv->size(),
      rv->estimateFlatSize(),
      freeMem.value() + pool()->freeBytes(),
      pool(),
      arrowPool_.get());
}

void SparkShuffleWriter::addInput(RowVectorPtr input) {
  Operator::ReclaimableSectionGuard guard(this);
  std::call_once(initOnceFlag_, [this, &input]() { this->init(input); });
  auto freeMem = ExecutionMemoryPool::getMinimumFreeMemoryForTask(
      shuffleWriterOptions_.taskAttemptId);
  BOLT_CHECK(
      freeMem.has_value(),
      "Expect ExecutionMemoryPool::getMinimumFreeMemoryForTask return value");
  auto memLimit = freeMem.value() + pool()->freeBytes();
  auto status = shuffleWriter_->split(input, memLimit);
  BOLT_CHECK(status.ok(), "Native split: shuffle writer split failed");
}

void SparkShuffleWriter::noMoreInput() {
  Operator::noMoreInput();
  ShuffleWriterMetrics metrics;
  if (shuffleWriter_) {
    auto status = shuffleWriter_->stop();
    BOLT_CHECK(status.ok(), "Native shuffle write: ShuffleWriter stop failed");
    metrics = shuffleWriter_->metrics();
  } else {
    metrics.partitionLengths = std::vector<int64_t>(
        shuffleWriterOptions_.partitionWriterOptions.numPartitions, 0);
    metrics.rawPartitionLengths = std::vector<int64_t>(
        shuffleWriterOptions_.partitionWriterOptions.numPartitions, 0);

    LOG(INFO) << "ShuffleWriter is null";
  }

  reportShuffleStatusCallback_(metrics);
}

RowVectorPtr SparkShuffleWriter::getOutput() {
  if (noMoreInput_) {
    finished_ = true;
  }
  return nullptr;
}

void SparkShuffleWriter::reclaim(
    uint64_t targetBytes,
    memory::MemoryReclaimer::Stats& stats) {
  int64_t evictedSize;
  if (shuffleWriter_) {
    auto status = shuffleWriter_->reclaimFixedSize(targetBytes, &evictedSize);
    BOLT_CHECK(status.ok(), "(shuffle) nativeEvict: evict failed");
  } else {
    LOG(INFO) << "ShuffleWriter is null when reclaim";
  }
}

void SparkShuffleWriter::close() {
  Operator::close();
}
