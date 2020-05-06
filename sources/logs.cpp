// Copyright 2018 Your Name <your_email>

#include <main.hpp>
#include <logs.hpp>

namespace logging = boost::log;
namespace src = boost::log::sources;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;
namespace sinks = boost::log::sinks;

void logs::logInFile() {
    logging::add_file_log
            (
                    keywords::file_name = "/log/info.log",
                    keywords::rotation_size = 256 * 1024 * 1024,
                    keywords::time_based_rotation =
                            sinks::file::rotation_at_time_point(0, 0, 0),
                    keywords::filter =
                            logging::trivial::severity
                            >= logging::trivial::info,
                    keywords::format =
                            (
                                    expr::stream
                                            << boost::posix_time
                                            ::second_clock::local_time()
                                            << " : <" << logging
                                            ::trivial::severity
                                            << "> " << expr::smessage));

    logging::add_file_log
            (
                    keywords::file_name = "/log/trace.log",
                    keywords::rotation_size = 256 * 1024 * 1024,
                    keywords::time_based_rotation = sinks::file
                    ::rotation_at_time_point(0, 0, 0),
                    keywords::filter = logging::trivial::severity
                                       >= logging::trivial::trace,
                    keywords::format =
                            (
                                    expr::stream
                                            << boost::posix_time
                                            ::second_clock::local_time()
                                            << " : <" << logging::
                                            trivial::severity
                                            << "> " << expr::smessage));
}

void logs::logInfo(const std::string &key, const std::string &hash) {
    BOOST_LOG_TRIVIAL(info) << "Thread with ID: "
                            << std::this_thread::get_id() << " Key: "
                            << key << " Hash: " << hash << std::endl;
}

void logs::logTrace(const std::string &key, const std::string &value) {
    BOOST_LOG_TRIVIAL(trace) << "Thread with ID: "
                             << std::this_thread::get_id() << " Key: "
                             << key << " Value: " << value << std::endl;
}


