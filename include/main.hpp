// Copyright 2018 Your Name <your_email>

#ifndef INCLUDE_MAIN_HPP_
#define INCLUDE_MAIN_HPP_
#include <iostream>
#include <string>
#include <random>
#include <utility>
#include <thread>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/unordered_map.hpp>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <boost/program_options.hpp>
#include <mutex>
#include <vector>
#include <list>
#include <picosha2.hpp>
#include <constants.hpp>

using FContainer =
        std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>>;
using FDescriptorContainer =
        std::vector<rocksdb::ColumnFamilyDescriptor>;
using FHandlerContainer =
        std::list<std::unique_ptr<rocksdb::ColumnFamilyHandle>>;
using StrContainer = boost::unordered_map<std::string, std::string>;
namespace po = boost::program_options;

class DBHashCreator {
public:
    explicit DBHashCreator(std::string path) : _path(path) {}

    DBHashCreator(std::string path,
            std::size_t threadCount,
            std::string logLVL) :
            _path(path),
            _logLVL(logLVL),  _threadCountHash(threadCount){}

    FDescriptorContainer getFamilyDescriptors();

    FHandlerContainer openDB(const FDescriptorContainer &);

    StrContainer getStrs(rocksdb::ColumnFamilyHandle *);

    void getHash(rocksdb::ColumnFamilyHandle *, StrContainer);

    void startHash(FHandlerContainer *, std::list<StrContainer> *);

    void startThreads();

private:
    std::string _path;
    std::string _logLVL;
    std::unique_ptr<rocksdb::DB> _db;
    std::size_t _threadCountHash = DEFAULT_THREAD_HASH;
    std::mutex _mutex;
};
#endif // INCLUDE_MAIN_HPP_
