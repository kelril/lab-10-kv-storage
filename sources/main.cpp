// Copyright 2018 Your Name <your_email>
#include <main.hpp>
#include <constants.hpp>
#include <logs.hpp>

int main(int argc, char **argv) {
    po::options_description desc("short description");
    desc.add_options()
            ("help,h", "0 помощи")
            ("log_level", po::value<std::string>(),
             "level logging")
            ("thread_count", po::value<unsigned>(),
             "count of threads")
            ("output", po::value<std::string>(),
             "path result");

    po::variables_map vm;
    try {
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);
    }

    catch (po::error &e) {
        std::cout << e.what() << std::endl;
        std::cout << desc << std::endl;
        return 1;
    }
    if (vm.count("help")) {
        std::cout << desc << std::endl;
        return 1;
    }
    if (!vm.count("log_level")
        || !vm.count("thread_count")
        || !vm.count("output")) {
        std::cout << "error: bad format" << std::endl << desc << std::endl;
        return 1;
    }

    std::string logLVL = vm["log_level"].as<std::string>();
    std::size_t threadCount = vm["thread_count"].as<unsigned>();
    std::string pathToFile = vm["output"].as<std::string>();

    logs::logInFile();
    DBHashCreator db(pathToFile, threadCount, logLVL);
    db.startThreads();
    return 0;
}
