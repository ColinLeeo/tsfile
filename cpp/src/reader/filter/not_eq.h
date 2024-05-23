#ifndef READER_FILTER_OPERATOR_NOT_EQ_H
#define READER_FILTER_OPERATOR_NOT_EQ_H

#include "filter/unary_filter.h"

namespace storage {
template <typename T>
class NotEq : public UnaryFilter<T> {
   public:
    NotEq() {}
    NotEq(T value, FilterType type) { UnaryFilter(value, type); }
    virtual ~NotEq() {}

    bool satisfy(Statistic *statistic) {
        if (type_ == TIME_FILTER) {
            return !(value_ == statistic->start_time_ &&
                     value_ == statistic->end_time_);
        } else {
            if (statistic.get_type() == common::TEXT ||
                statistic.get_type() == common::BOOLEAN) {
                return true;
            } else {
                // todo value filter
                return true;
            }
        }
    }

    bool satisfy(long time, Object value) {
        Object v = (type_ == TIME_FILTER ? time : value);
        return !value_.equals(v);
    }

    bool satisfy_start_end_time(long start_time, long end_time) {
        if (filterType == TIME_FILTER) {
            return value_ != end_time && value_ != start_time;
        } else {
            return true;
        }
    }

    bool contain_start_end_time(long start_time, long end_time) {
        if (filterType == TIME_FILTER) {
            return value_ < start_time || value_ > end_time;
        } else {
            return true;
        }
    }
};

}  // namespace storage

#endif  // READER_FILTER_OPERATOR_NOT_EQ_H