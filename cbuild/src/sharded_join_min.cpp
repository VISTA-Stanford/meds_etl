// Ultra-minimal sharded on-disk join with schema-only JSON config and root-dir discovery.
// Supports .csv and .csv.gz (zlib). Minimal output schema:
//   primary_key,start_datetime,end_datetime,json_payload
// Build: g++ -O3 -std=gnu++17 -pthread -o sharded_join_min sharded_join_min.cpp -lz

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <unistd.h>    // getpid()
#include <zlib.h>      // gzopen, gzgets

namespace fs = std::filesystem;

/*==================== Small helpers ====================*/
static inline bool ends_with(const std::string& s, const std::string& suf) {
    return s.size() >= suf.size() && std::equal(s.end()-suf.size(), s.end(), suf.begin());
}
static inline uint64_t hash64(const std::string& s) {
    uint64_t h=1469598103934665603ull;
    for (unsigned char c: s){ h^=c; h*=1099511628211ull; }
    return h;
}
static inline bool is_digits(const std::string& s){
    if (s.empty()) return false; for(char c:s) if(c<'0'||c>'9') return false; return true;
}
// Parse ts: digits -> epoch ms; else ISO8601 "YYYY-mm-ddTHH:MM:SS[.sss][Z]" (UTC)
static inline std::optional<int64_t> parse_ts_ms(const std::string& s){
    if (s.empty()) return std::nullopt;
    if (is_digits(s)) return (int64_t)std::stoll(s); // epoch ms
    std::string base=s;
    auto pos=base.find('.'); if(pos!=std::string::npos){ auto z=base.find('Z',pos); if(z!=std::string::npos) base.erase(pos,z-pos); else base.erase(pos); }
    if(!base.empty() && base.back()=='Z') base.pop_back();
    std::tm tm{}; std::istringstream iss(base); iss>>std::get_time(&tm,"%Y-%m-%dT%H:%M:%S"); if(iss.fail()) return std::nullopt;
    time_t secs = timegm(&tm); if (secs<0) return std::nullopt;
    return (int64_t)secs*1000;
}
static inline std::string json_escape(const std::string& s){
    std::string o; o.reserve(s.size()+8);
    for (unsigned char uc: s){ char c=(char)uc;
        switch(c){
            case '\"': o+="\\\""; break; case '\\': o+="\\\\"; break;
            case '\b': o+="\\b"; break; case '\f': o+="\\f"; break;
            case '\n': o+="\\n"; break; case '\r': o+="\\r"; break; case '\t': o+="\\t"; break;
            default: if (uc<0x20){ char buf[7]; std::snprintf(buf,sizeof(buf),"\\u%04x",uc); o+=buf; } else o+=c;
        }
    } return o;
}
static inline std::string to_rfc3339_utc(int64_t ms){
    time_t secs = (time_t)(ms / 1000);
    std::tm tm{}; gmtime_r(&secs, &tm);
    char buf[32]; std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return std::string(buf);
}

/*==================== GZ line reader ====================*/
// read one line from gzFile into `out` (no trailing '\n'); returns false at EOF
static bool gz_getline(gzFile gz, std::string& out) {
    out.clear();
    if (!gz) return false;
    const int CHUNK = 1 << 15; // 32KB
    std::string buf; buf.resize(CHUNK);
    bool any = false;
    for (;;) {
        char* res = gzgets(gz, buf.data(), CHUNK);
        if (!res) return any;
        any = true;
        size_t got = std::strlen(res);
        char* nl = (char*)memchr(res, '\n', got);
        if (nl) {
            size_t keep = nl - res;
            out.append(res, keep);
            break;
        } else {
            out.append(res, got);
            if (got < (size_t)(CHUNK-1)) break; // likely EOF
        }
    }
    if (!out.empty() && out.back()=='\r') out.pop_back();
    return true;
}

/*==================== Tiny CSV ====================*/
struct CsvReader {
    std::ifstream in;
    gzFile gz = nullptr;
    bool is_gz = false;
    std::vector<std::string> headers;

    explicit CsvReader(const std::string& path, bool has_headers=true) {
        is_gz = ends_with(path, ".gz");
        if (is_gz) {
            gz = gzopen(path.c_str(), "rb");
            if (!gz) throw std::runtime_error("open failed: " + path);
        } else {
            in.open(path);
            if (!in) throw std::runtime_error("open failed: " + path);
        }
        if (has_headers) {
            std::string line;
            if (!getline_any(line)) throw std::runtime_error("empty csv: " + path);
            headers = parse_line(line);
        }
    }
    ~CsvReader(){ if (gz) gzclose(gz); if (in.is_open()) in.close(); }

    static std::vector<std::string> parse_line(const std::string& line) {
        std::vector<std::string> out;
        std::string cur; bool inq=false;
        for (size_t i=0;i<line.size();++i) {
            char c=line[i];
            if (inq) {
                if (c=='"') { if (i+1<line.size() && line[i+1]=='"'){cur.push_back('"');++i;} else inq=false; }
                else cur.push_back(c);
            } else {
                if (c==','){ out.push_back(cur); cur.clear(); }
                else if (c=='"'){ inq=true; }
                else cur.push_back(c);
            }
        }
        out.push_back(cur);
        return out;
    }
    bool next(std::vector<std::string>& row) {
        row.clear(); std::string line;
        if (!getline_any(line)) return false;
        row = parse_line(line);
        return true;
    }
private:
    bool getline_any(std::string& line) {
        if (is_gz) return gz_getline(gz, line);
        if (!std::getline(in, line)) return false;
        if (!line.empty() && line.back()=='\r') line.pop_back();
        return true;
    }
};

/*==================== JSON (tiny subset) ====================*/
struct JVal {
    enum T{OBJ, ARR, STR, NUM, NUL, BOOL} t=NUL;
    std::map<std::string,JVal> obj;
    std::vector<JVal> arr;
    std::string str;
    double num=0.0;
    bool b=false;
};
struct JTok {
    const std::string s; size_t i=0;
    JTok(const std::string& s_):s(s_) {}
    void ws(){ while(i<s.size() && (s[i]==' '||s[i]=='\n'||s[i]=='\t'||s[i]=='\r')) ++i; }
    bool eat(char c){ ws(); if(i<s.size() && s[i]==c){++i; return true;} return false; }
    std::string parse_string(){
        ws(); if(i>=s.size()||s[i]!='"') throw std::runtime_error("json: expected string");
        ++i; std::string out;
        while(i<s.size()){
            char c=s[i++];
            if(c=='"') break;
            if(c=='\\'){
                if(i>=s.size()) throw std::runtime_error("json: bad escape");
                char e=s[i++];
                switch(e){ case '"': out.push_back('"'); break; case '\\': out.push_back('\\'); break;
                    case '/': out.push_back('/'); break; case 'b': out.push_back('\b'); break;
                    case 'f': out.push_back('\f'); break; case 'n': out.push_back('\n'); break;
                    case 'r': out.push_back('\r'); break; case 't': out.push_back('\t'); break;
                    case 'u': { if(i+4> s.size()) throw std::runtime_error("json: bad \\u"); out += "\\u"; out.append(s.substr(i,4)); i+=4; break; }
                    default: throw std::runtime_error("json: bad escape");
                }
            } else out.push_back(c);
        }
        return out;
    }
    JVal parse_value(){
        ws(); if(i>=s.size()) throw std::runtime_error("json: eof");
        if(s[i]=='{') return parse_object();
        if(s[i]=='[') return parse_array();
        if(s[i]=='"'){ JVal v; v.t=JVal::STR; v.str=parse_string(); return v; }
        if(s.compare(i,4,"true")==0){ i+=4; JVal v; v.t=JVal::BOOL; v.b=true; return v; }
        if(s.compare(i,5,"false")==0){ i+=5; JVal v; v.t=JVal::BOOL; v.b=false; return v; }
        if(s.compare(i,4,"null")==0){ i+=4; JVal v; v.t=JVal::NUL; return v; }
        size_t j=i; if(s[i]=='-') ++j;
        while(j<s.size() && ((s[j]>='0'&&s[j]<='9')||s[j]=='.'||s[j]=='e'||s[j]=='E'||s[j]=='+'||s[j]=='-')) ++j;
        if(j>i){ JVal v; v.t=JVal::NUM; v.num = std::stod(s.substr(i,j-i)); i=j; return v; }
        throw std::runtime_error("json: bad value");
    }
    JVal parse_object(){
        JVal v; v.t=JVal::OBJ; eat('{');
        ws(); if(eat('}')) return v;
        while(true){
            std::string k = parse_string();
            if(!eat(':')) throw std::runtime_error("json: expected :");
            JVal val = parse_value();
            v.obj.emplace(std::move(k), std::move(val));
            if(eat('}')) break;
            if(!eat(',')) throw std::runtime_error("json: expected ,");
        }
        return v;
    }
    JVal parse_array(){
        JVal v; v.t=JVal::ARR; eat('[');
        ws(); if(eat(']')) return v;
        while(true){
            v.arr.push_back(parse_value());
            if(eat(']')) break;
            if(!eat(',')) throw std::runtime_error("json: expected ,");
        }
        return v;
    }
};
static JVal json_parse_file(const std::string& path){
    std::ifstream in(path);
    if(!in) throw std::runtime_error("open json failed: "+path);
    std::string s((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    JTok t(s); JVal v = t.parse_value(); t.ws(); if(t.i!=s.size()) throw std::runtime_error("json: trailing chars");
    return v;
}

/*==================== Schema config ====================*/
struct Table { std::string name, ts_start, ts_end; };
struct Config {
    std::string primary_key="primary_key";
    std::map<std::string, Table> tables; // by name
};
static Config load_config(const std::string& path){
    JVal root = json_parse_file(path);
    if(root.t!=JVal::OBJ) throw std::runtime_error("config: top-level must be object");
    Config c;
    if(auto it=root.obj.find("primary_key"); it!=root.obj.end() && it->second.t==JVal::STR) c.primary_key = it->second.str;
    auto itt = root.obj.find("tables");
    if(itt==root.obj.end() || itt->second.t!=JVal::ARR) throw std::runtime_error("config: tables[] required");
    for(const auto& tv : itt->second.arr){
        if(tv.t!=JVal::OBJ) continue;
        Table t;
        if(auto f=tv.obj.find("name"); f!=tv.obj.end() && f->second.t==JVal::STR) t.name=f->second.str;
        if(auto f=tv.obj.find("ts_start"); f!=tv.obj.end() && f->second.t==JVal::STR) t.ts_start=f->second.str;
        if(auto f=tv.obj.find("ts_end"); f!=tv.obj.end() && f->second.t==JVal::STR) t.ts_end=f->second.str;
        if(t.name.empty()||t.ts_start.empty()) throw std::runtime_error("table needs name and ts_start");
        c.tables.emplace(t.name, t);
    }
    return c;
}

/*==================== Discover CSV(.gz) under root ====================*/
static std::vector<std::string> find_csvs_under(const fs::path& dir){
    std::vector<std::string> files;
    if(!fs::exists(dir)) return files;
    for (auto const& de : fs::recursive_directory_iterator(dir, fs::directory_options::follow_directory_symlink)) {
        if (!de.is_regular_file()) continue;
        auto p = de.path().string();
        if (ends_with(p, ".csv") || ends_with(p, ".csv.gz")) files.push_back(std::move(p));
    }
    return files;
}

/*==================== Events & I/O ====================*/
struct EventRow {
    std::string pk;
    int64_t ts_start_ms=0;
    int64_t ts_end_ms=0;      // 0 if missing
    std::string tie;          // table:file:line
    std::string payload_json; // includes _event_type
    bool operator<(const EventRow& o) const {
        if (pk != o.pk) return pk < o.pk;
        if (ts_start_ms != o.ts_start_ms) return ts_start_ms < o.ts_start_ms;
        return tie < o.tie;
    }
};
struct HeapItem {
    EventRow row;
    size_t src_idx;  // which RunReader produced this row

    // std::priority_queue is a max-heap; invert comparisons to behave like a min-heap
    bool operator<(const HeapItem& o) const {
        if (row.pk != o.row.pk)             return row.pk > o.row.pk;
        if (row.ts_start_ms != o.row.ts_start_ms) return row.ts_start_ms > o.row.ts_start_ms;
        return row.tie > o.row.tie;
    }
};
static std::atomic<uint64_t> g_seq{0};

static void write_run(const fs::path& p, const std::vector<EventRow>& v){
    fs::create_directories(p.parent_path());
    std::ofstream out(p);
    if(!out) throw std::runtime_error("write failed: "+p.string());
    for(const auto& r: v){
        out<<r.pk<<","<<r.ts_start_ms<<","<<r.ts_end_ms<<","<<r.tie<<","<<r.payload_json<<"\n";
    }
    out.flush();
}

/*==================== CSV helpers ====================*/
static size_t find_col(const std::vector<std::string>& hdr, const std::string& name){
    for(size_t i=0;i<hdr.size();++i) if(hdr[i]==name) return i;
    return SIZE_MAX;
}
static inline std::string row_to_json(const std::vector<std::string>& headers,
                                      const std::vector<std::string>& row,
                                      const std::string& event_type){
    std::ostringstream os; os<<'{';
    for(size_t i=0;i<headers.size()&&i<row.size();++i){
        if(i) os<<','; os<<'"'<<json_escape(headers[i])<<"\":\""<<json_escape(row[i])<<'"';
    }
    os<<",\"_event_type\":\""<<json_escape(event_type)<<"\"}";
    return os.str();
}

/*==================== Pass 1 ====================*/
struct PArgs {
    std::string pk_col;
    size_t shards;
    size_t rows_per_run;
    fs::path tmp_root;
};
static void partition_one_file(const std::string& table_name,
                               const Table& tspec,
                               const std::string& file,
                               size_t src_idx,
                               const PArgs& a)
{
    CsvReader rdr(file, true);
    auto hdr = rdr.headers;
    size_t pk_i = find_col(hdr, a.pk_col);
    if (pk_i==SIZE_MAX){ std::cerr<<"skip "<<file<<" (missing pk)\n"; return; }
    size_t s_i = find_col(hdr, tspec.ts_start);
    size_t e_i = tspec.ts_end.empty()? SIZE_MAX : find_col(hdr, tspec.ts_end);

    std::vector<std::vector<EventRow>> buf(a.shards);
    std::vector<std::string> row; uint64_t line_no=0;

    while(rdr.next(row)){
        ++line_no;
        if (pk_i>=row.size()) continue;
        const std::string& pk = row[pk_i]; if(pk.empty()) continue;

        int64_t tstart=0, tend=0; bool ok=false;
        if (s_i!=SIZE_MAX && s_i<row.size()){ auto v=parse_ts_ms(row[s_i]); if(v){ tstart=*v; ok=true; } }
        if (!ok) continue; // must have start
        if (e_i!=SIZE_MAX && e_i<row.size()){ auto v=parse_ts_ms(row[e_i]); if(v){ tend=*v; } }

        EventRow ev;
        ev.pk = pk; ev.ts_start_ms=tstart; ev.ts_end_ms=tend;
        ev.tie = table_name + ":" + file + ":" + std::to_string(line_no);
        ev.payload_json = row_to_json(hdr,row,table_name);

        size_t shard = (size_t)(hash64(pk) % a.shards);
        auto& v = buf[shard]; v.emplace_back(std::move(ev));
        if (v.size() >= a.rows_per_run){
            std::sort(v.begin(), v.end());
            fs::path out = a.tmp_root / ("shard="+std::to_string(shard)) / table_name
                         / ("src="+std::to_string(src_idx))
                         / ("run-"+std::to_string(getpid())+"-"+std::to_string(g_seq++)+".csv");
            write_run(out, v);
            v.clear(); v.shrink_to_fit();
        }
    }
    for(size_t s=0;s<buf.size();++s) if(!buf[s].empty()){
        std::sort(buf[s].begin(), buf[s].end());
        fs::path out = a.tmp_root / ("shard="+std::to_string(s)) / table_name
                     / ("src="+std::to_string(src_idx))
                     / ("run-"+std::to_string(getpid())+"-"+std::to_string(g_seq++)+".csv");
        write_run(out, buf[s]); buf[s].clear();
    }
}

/*==================== Pass 2 ====================*/
struct RunReader {
    std::ifstream in; std::string path;
    RunReader(const std::string& p): in(p), path(p){ if(!in) throw std::runtime_error("open run failed: "+p); }
    bool next(EventRow& r){
        std::string line; if(!std::getline(in,line)) return false;
        auto c = CsvReader::parse_line(line); if(c.size()<5) return false;
        r.pk=c[0]; r.ts_start_ms=std::stoll(c[1]); r.ts_end_ms=std::stoll(c[2]); r.tie=c[3]; r.payload_json=c[4]; return true;
    }
};

static void merge_shard(const fs::path& tmp_root, const fs::path& out_root, size_t shard, size_t max_open){
    fs::path root = tmp_root / ("shard="+std::to_string(shard)); if(!fs::exists(root)) return;
    std::vector<std::string> runs;
    for (auto& tbl: fs::directory_iterator(root)) if(tbl.is_directory()){
        for (auto& src: fs::directory_iterator(tbl.path())) if(src.is_directory()){
            for (auto& f: fs::directory_iterator(src.path()))
                if (f.is_regular_file() && f.path().extension()==".csv") runs.push_back(f.path().string());
        }
    }
    if(runs.empty()) return;

    auto tier = [&](std::vector<std::string> in)->std::vector<std::string>{
        if (in.size()<=max_open) return in;
        std::vector<std::string> out;
        for(size_t i=0;i<in.size();i+=max_open){
            size_t j=std::min(i+max_open,in.size());
            fs::path outp = tmp_root / ("shard="+std::to_string(shard)) / ("merged-"+std::to_string(g_seq++)+".csv");
            std::ofstream w(outp); if(!w) throw std::runtime_error("merge open failed");
            std::vector<std::unique_ptr<RunReader>> rs;
            for(size_t k=i;k<j;++k) rs.emplace_back(new RunReader(in[k]));
            std::priority_queue<HeapItem> heap;
            for(size_t k=0;k<rs.size();++k){ EventRow r; if(rs[k]->next(r)) heap.push(HeapItem{r,k}); }
            while(!heap.empty()){
                auto it=heap.top(); heap.pop();
                auto& r = it.row;
                w<<r.pk<<","<<r.ts_start_ms<<","<<r.ts_end_ms<<","<<r.tie<<","<<r.payload_json<<"\n";
                if(rs[it.src_idx]->next(r)) heap.push(HeapItem{r,it.src_idx});
            }
            w.flush();
            for(size_t k=i;k<j;++k){ std::error_code ec; fs::remove(in[k],ec); }
            out.push_back(outp.string());
        }
        return out;
    };
    while(runs.size()>max_open) runs = tier(runs);

    fs::path inprog = out_root / ("shard="+std::to_string(shard)+".inprogress");
    std::error_code ec; fs::remove_all(inprog,ec); fs::create_directories(inprog);
    std::ofstream out(inprog / "part-0000.csv");
    out<<"primary_key,start_datetime,end_datetime,json_payload\n";

    std::vector<std::unique_ptr<RunReader>> rs; for(auto& p:runs) rs.emplace_back(new RunReader(p));
    std::priority_queue<HeapItem> heap;
    for(size_t k=0;k<rs.size();++k){ EventRow r; if(rs[k]->next(r)) heap.push(HeapItem{r,k}); }
    while(!heap.empty()){
        auto it=heap.top(); heap.pop(); auto& r=it.row;
        out<<r.pk<<","<<to_rfc3339_utc(r.ts_start_ms)<<","<<(r.ts_end_ms>0?to_rfc3339_utc(r.ts_end_ms):"")<<","<<r.payload_json<<"\n";
        if(rs[it.src_idx]->next(r)) heap.push(HeapItem{r,it.src_idx});
    }
    out.flush();
    fs::path final_dir = out_root / ("shard="+std::to_string(shard));
    fs::remove_all(final_dir,ec); fs::rename(inprog, final_dir);
}
// struct HeapItem { EventRow row; size_t src_idx;
//     bool operator<(const HeapItem& o) const {
//         if (row.pk != o.row.pk) return row.pk > o.row.pk;
//         if (row.ts_start_ms != o.row.ts_start_ms) return row.ts_start_ms > o.row.ts_start_ms;
//         return row.tie > o.row.tie;
//     }
// };

/*==================== CLI ====================*/
struct Cli {
    std::string config_json;
    fs::path root;   // root dir containing subdirs named after tables
    size_t shards=256;
    fs::path tmp="tmp";
    fs::path out="final";
    size_t workers=std::thread::hardware_concurrency();
    size_t rows_per_run=250000;
    size_t max_open=1024;
};
static void usage(const char* p){
    std::cerr<<"Usage: "<<p<<" --config config.json --root /path/to/root --shards 512 "
             <<"[--tmp tmp] [--out final] [--workers 112] [--rows_per_run 250000] [--max_open 2048]\n";
}
static Cli parse_cli(int argc, char** argv){
    Cli c;
    for(int i=1;i<argc;i++){
        std::string a=argv[i];
        auto need=[&](const char*){ if(i+1>=argc){usage(argv[0]); std::exit(1);} return std::string(argv[++i]); };
        if(a=="--config") c.config_json = need("--config");
        else if(a=="--root") c.root = need("--root");
        else if(a=="--shards") c.shards = std::stoul(need("--shards"));
        else if(a=="--tmp") c.tmp = need("--tmp");
        else if(a=="--out") c.out = need("--out");
        else if(a=="--workers") c.workers = std::stoul(need("--workers"));
        else if(a=="--rows_per_run") c.rows_per_run = std::stoul(need("--rows_per_run"));
        else if(a=="--max_open") c.max_open = std::stoul(need("--max_open"));
        else { usage(argv[0]); std::exit(1); }
    }
    if(c.config_json.empty() || c.root.empty()){ usage(argv[0]); std::exit(1); }
    return c;
}

/*==================== Main ====================*/
int main(int argc, char** argv){
    try{
        Cli cli = parse_cli(argc, argv);
        Config cfg = load_config(cli.config_json);

        fs::create_directories(cli.tmp);
        fs::create_directories(cli.out);

        // Enumerate jobs by scanning root/<table_name>/**.{csv,csv.gz}
        struct Job{ std::string tbl; Table tspec; std::string file; size_t src_idx; };
        std::vector<Job> jobs;
        for (const auto& [tbl_name, tspec] : cfg.tables) {
            fs::path tbl_root = cli.root / tbl_name;
            auto files = find_csvs_under(tbl_root);
            if (files.empty()) {
                std::cerr << "warn: no CSVs under " << tbl_root << " for table '"<<tbl_name<<"'\n";
                continue;
            }
            for (size_t i=0;i<files.size();++i) jobs.push_back(Job{tbl_name, tspec, files[i], i});
        }

        // Pass 1
        struct PArgs pargs{cfg.primary_key, cli.shards, cli.rows_per_run, cli.tmp};
        std::atomic<size_t> next{0};
        size_t W = std::max<size_t>(1, cli.workers);
        std::vector<std::thread> pool; pool.reserve(W);
        for(size_t w=0; w<W; ++w){
            pool.emplace_back([&](){
                while(true){
                    size_t i = next.fetch_add(1);
                    if(i>=jobs.size()) break;
                    auto& j = jobs[i];
                    try{ partition_one_file(j.tbl, j.tspec, j.file, j.src_idx, pargs); }
                    catch(const std::exception& e){ std::cerr<<"partition error: "<<e.what()<<"\n"; }
                }
            });
        }
        for(auto& th: pool) th.join();

        // Pass 2
        size_t R = std::min(cli.shards, std::max<size_t>(1, cli.workers/4));
        std::atomic<size_t> ns{0};
        std::vector<std::thread> reducers; reducers.reserve(R);
        for(size_t r=0;r<R;++r){
            reducers.emplace_back([&](){
                while(true){
                    size_t s = ns.fetch_add(1);
                    if(s>=cli.shards) break;
                    try{ merge_shard(cli.tmp, cli.out, s, cli.max_open); }
                    catch(const std::exception& e){ std::cerr<<"reduce shard "<<s<<" error: "<<e.what()<<"\n"; }
                }
            });
        }
        for(auto& th: reducers) th.join();

        std::cerr<<"Done.\n";
        return 0;
    } catch(const std::exception& e){
        std::cerr<<"fatal: "<<e.what()<<"\n"; return 1;
    }
}
