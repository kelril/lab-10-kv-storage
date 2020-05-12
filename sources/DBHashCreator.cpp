// Copyright 2018 Your Name <your_email>
#include <main.hpp>
#include <constants.hpp>
#include <logs.hpp>

FHandlerContainer DBHashCreator::openDB
        (const FDescriptorContainer &descriptors) {
    FHandlerContainer handlers;
    std::vector < rocksdb::ColumnFamilyHandle * > newHandles;
    rocksdb::DB *dbStrPtr;

    rocksdb::Status status =
            rocksdb::DB::Open(
                    rocksdb::DBOptions(),
                    _path,
                    descriptors,
                    &newHandles,
                    &dbStrPtr);
    assert(status.ok()); //if 0 -> exit

    _db.reset(dbStrPtr);

    for (rocksdb::ColumnFamilyHandle *ptr : newHandles) {
        handlers.emplace_back(ptr);
    }

    return handlers;
}

FDescriptorContainer DBHashCreator::getFamilyDescriptors() {
    rocksdb::Options options;

    std::vector <std::string> family;
    FDescriptorContainer descriptors;
    rocksdb::Status status =
            rocksdb::DB::ListColumnFamilies(rocksdb::DBOptions(),
                                            _path,
                                            &family);
    assert(status.ok()); //if 0 -> exit

    for (const std::string &familyName : family) {
        descriptors.emplace_back(familyName,
                                 rocksdb::ColumnFamilyOptions());
    }
    return descriptors;
}

StrContainer DBHashCreator::getStrs(rocksdb::ColumnFamilyHandle *family) {
    boost::unordered_map <std::string, std::string> dbCase;
    std::unique_ptr <rocksdb::Iterator>
            it(_db->NewIterator(rocksdb::ReadOptions(), family));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        dbCase[key] = value;
    }
    return dbCase;
}

void DBHashCreator::getHash
        (rocksdb::ColumnFamilyHandle *family, StrContainer strContainer) {
    for (auto it = strContainer.begin(); it != strContainer.end(); ++it) {
        std::string hash = picosha2::hash256_hex_string(it->first + it->second);
        std::cout << "key: " << it->first << " hash: " << hash << std::endl;
        logs::logInfo(it->first, hash);
        rocksdb::Status status = _db->Put(rocksdb::WriteOptions(),
                                          family,
                                          it->first,
                                          hash);
        assert(status.ok());
    }
}

void DBHashCreator::startHash
        (FHandlerContainer *handlers,
                std::list <StrContainer> *StrContainerList) {
    while (!handlers->empty()) {
        _mutex.lock();
        if (handlers->empty()) {
            _mutex.unlock();
            continue;
        }
        auto &family = handlers->front();
        handlers->pop_front();

        StrContainer strContainer = StrContainerList->front();
        StrContainerList->pop_front();
        _mutex.unlock();
        getHash(family.get(), strContainer);
    }
}

void DBHashCreator::startThreads() {
    auto deskriptors = getFamilyDescriptors();
    auto handlers = openDB(deskriptors);

    std::list <StrContainer> StrContainerList;

    for (auto &family : handlers) {
        StrContainerList.push_back(
                getStrs(family.get()));
    }

    std::vector <std::thread> threads;
    threads.reserve(_threadCountHash);
    for (size_t i = 0; i < _threadCountHash; ++i) {
        threads.emplace_back(std::thread
                                     (&DBHashCreator::startHash,
                                      this,
                                      &handlers,
                                      &StrContainerList));
    }
    for (auto &th : threads) {
        th.join();
    }
}
