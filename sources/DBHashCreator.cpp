// Copyright 2018 Your Name <your_email>
#include <main.hpp>
#include <constants.hpp>
#include <logs.hpp>

// Open DB with column families.
// db_options specify database specific options
// column_families is the vector of all column families in the database,
// containing column family name and options. You need to open ALL column
// families in the database. To get the list of column families, you can use
// ListColumnFamilies(). Also, you can open only a subset of column families
// for read-only access.
// The default column family name is 'default' and it's stored
// in ROCKSDB_NAMESPACE::kDefaultColumnFamilyName.
// If everything is OK, handles will on return be the same size
// as column_families --- handles[i] will be a handle that you
// will use to operate on column family column_family[i].
// Before delete DB, you have to close All column families by calling
// DestroyColumnFamilyHandle() with all the handles.
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

void DBHashCreator::createDB() {
    //maybe remove
    rocksdb::DB *dbPtr;
    rocksdb::Options options;
    options.create_if_missing = true; //if directory not exist

    rocksdb::Status status = rocksdb::DB::Open(options, _path, &dbPtr);
    assert(status.ok()); //if 0 -> exit

    _db.reset(dbPtr);
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

//std::random_device: uniformly distributed random number generator.
//std::mt19937: random number engine based on the Mersenne Twister algorithm.
//std::uniform_int_distribution: distribution for random integer values between
// two bounds in a closed interval.
void DBHashCreator::randomFillStrings(const FContainer &container) {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    for (auto &family : container) {
        for (size_t i = 0; i < STR_COUNT; ++i) {
            std::string key = getRandomString(KEY_LENGTH);
            std::string value = getRandomString(VALUE_LENGTH);
            std::cout << "key: " << key << " value: " << value << std::endl;
            logs::logTrace(key, value);
            rocksdb::Status status = _db->Put(rocksdb::WriteOptions(),
                                              family.get(),
                                              key,
                                              value);
            assert(status.ok());
        }
    }
}

std::string DBHashCreator::getRandomString(std::size_t length) {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    std::string random_string;

    for (std::size_t i = 0; i < length; ++i) {
        random_string += CHARACTERS[distribution(generator)];
    }

    return random_string;
}

FContainer DBHashCreator::randomFillFamilies() {
    std::random_device random_device;
    std::mt19937 generator(random_device());
    std::uniform_int_distribution<> distribution(0, CHARACTERS.size() - 1);

    FContainer family{};
    for (std::size_t i = 0; i < FAMILY_COUNT; ++i) {
        std::string familyName = getRandomString(FAMILY_NAME_LENGTH);
        std::cout << "Family: " << familyName << std::endl;
        rocksdb::ColumnFamilyHandle *familyStrPtr;

        rocksdb::Status status = _db->CreateColumnFamily(
                rocksdb::ColumnFamilyOptions(),
                familyName,
                &familyStrPtr);
        assert(status.ok());
        family.emplace_back(familyStrPtr);
    }
    return family;
}

void DBHashCreator::randomFill() {
    auto family = randomFillFamilies();

    randomFillStrings(family);
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
