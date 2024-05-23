
#ifndef READER_QDS_WITHOUT_TIMEGENERATOR_H
#define READER__QDS_WITHOUT_TIMEGENERATOR_H

#include <map>
#include <vector>

#include "expression.h"
#include "file/tsfile_io_reader.h"
#include "query_data_set.h"

namespace storage {

class QDSWithoutTimeGenerator : public QueryDataSet {
   public:
    QDSWithoutTimeGenerator()
        : row_record_(nullptr),
          io_reader_(nullptr),
          qe_(nullptr),
          ssi_vec_(),
          tsblocks_(),
          time_iters_(),
          value_iters_(),
          heap_time_() {}
    ~QDSWithoutTimeGenerator() { destroy(); }
    int init(TsFileIOReader *io_reader, QueryExpression *qe);
    void destroy();
    RowRecord *get_next();

   private:
    int get_next_tsblock(uint32_t index, bool alloc_mem);

   private:
    RowRecord *row_record_;
    TsFileIOReader *io_reader_;
    QueryExpression *qe_;
    std::vector<TsFileSeriesScanIterator *> ssi_vec_;
    std::vector<common::TsBlock *> tsblocks_;
    std::vector<common::ColIterator *> time_iters_;
    std::vector<common::ColIterator *> value_iters_;
    std::multimap<int64_t, uint32_t>
        heap_time_;  // key-->time, value-->path_index
};

}  // namespace storage

#endif  // READER_QDS_WITHOUT_TIMEGENERATOR_H
