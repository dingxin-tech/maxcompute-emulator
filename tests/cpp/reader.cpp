// Exercise the unmodified public SDK's Tunnel CRC stream with Arrow's IPC reader.
#include "tunnel/arrow_http_stream.h"
#include <arrow/ipc/reader.h>
#include <fstream>
#include <iostream>
#include <cstring>
class FileReader {
    std::ifstream file;
public:
    explicit FileReader(const char* path): file(path, std::ios::binary) {}
    int64_t Read(char* out, int64_t size) {
        file.read(out, size);
        return file.gcount();
    }
};
int main(int argc, char** argv) {
    if (argc != 3) return 2;
    auto schema = arrow::schema({arrow::field("id", arrow::int64()), arrow::field("s", arrow::utf8())});
    auto source = std::make_shared<FileReader>(argv[1]);
    apsara::odps::sdk::internal::tunnel::ArrowInputStream<FileReader> input(source);
    int64_t rows=0, sum=0, batches=0;
    while (true) {
        auto result = arrow::ipc::ReadRecordBatch(schema, nullptr, arrow::ipc::IpcReadOptions::Defaults(), &input);
        if (!result.ok()) {
            if (input.IsEOF()) break;
            std::cerr << result.status().ToString() << "\n"; return 3;
        }
        auto batch = result.ValueOrDie();
        if (!batch) break;
        if (!batch->num_rows()) return 4;
        ++batches;
        auto ids = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
        auto strs = std::static_pointer_cast<arrow::StringArray>(batch->column(1));
        for (int64_t i=0; i<batch->num_rows(); ++i) {
            if (ids->Value(i)!=rows || strs->GetString(i)!="value"+std::to_string(rows)) return 5;
            sum += ids->Value(i); ++rows;
        }
    }
    if (rows!=std::stoll(argv[2])) { std::cerr<<"short read "<<rows<<"\n"; return 6; }
    std::cout<<"rows="<<rows<<" uniq="<<rows<<" sum="<<sum<<" batches="<<batches<<"\n";
}
