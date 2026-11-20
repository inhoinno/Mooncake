// Hardware-free tests of the production admission/refill algorithm.
#include "write_pipeline.h"
#include <iostream>
#include <cassert>
#include <map>

namespace wp = mooncake::write_pipeline;
struct State { size_t submitted=0, live=0, peak=0, destroyed=0; bool fail=false; };
class Pending final : public wp::Operation {
    State& s_; size_t id_; bool done_=false; bool reordered_;
 public:
    Pending(State& s,size_t id,bool reordered):s_(s),id_(id),reordered_(reordered) {
        ++s_.live; s_.peak=std::max(s_.peak,s_.live);
    }
    ~Pending() override { if (!done_) --s_.live; ++s_.destroyed; }
    bool poll(wp::Completion& c) override {
        if (reordered_ && id_==0 && s_.submitted<3) return false;
        c.success=!(s_.fail && id_==1);
        done_=true; --s_.live; return true;
    }
};
std::vector<wp::Request> requests(size_t count=4) {
    std::vector<wp::Request> rows;
    auto lease=std::make_shared<int>(42);
    for(size_t i=0;i<count;++i)
        rows.push_back({i,4096,8192+i*4,4,"receiver",{"source",0,4},{"target",i*4,4},lease});
    return rows;
}
int main() {
    auto rows=requests();
    wp::Options o; o.max_operations=2; o.max_bytes=8;
    State s;
    bool released=false;
    auto result=wp::run(rows,o,[&](const auto&) {
        assert(released);
        const auto id=s.submitted++;
        return std::make_unique<Pending>(s,id,true);
    },{},[&]{assert(s.submitted==0);released=true;});
    assert(result.success && result.bytes==16 && s.peak==2 && s.live==0);
    assert(result.samples[0].request==1); // request 2 had to submit before A could complete
    assert(result.peak_bytes==8 && result.peak_operations==2);
    auto serial=wp::Options{}; serial.max_bytes=8;
    s={}; result=wp::run(rows,serial,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);});
    assert(result.success && s.peak==1 && result.bytes==16);
    auto alias=rows;
    for(auto& r:alias)r.destination_range.offset=0; // distinct VAs, same backing bytes
    s={}; result=wp::run(alias,o,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);});
    assert(result.success && s.peak==1 && result.conflict_wait_passes>0);
    o.max_bytes=4;
    s={}; result=wp::run(rows,o,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);});
    assert(result.success && s.peak==1 && result.peak_bytes==4);
    o.max_bytes=16; o.descriptors_per_operation=3;
    s={}; result=wp::run(rows,o,[&](const auto& b){
        assert(b.size()==3 || b.size()==1);
        return std::make_unique<Pending>(s,s.submitted++,false);
    });
    assert(result.success && result.operations==2 && result.samples.size()==4);
    o.descriptors_per_operation=1; s={}; s.fail=true;
    result=wp::run(rows,o,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);});
    assert(!result.success && s.submitted==2 && s.live==0);
    s={}; bool threw=false;
    try { wp::run(rows,o,[&](const auto&)->std::unique_ptr<wp::Operation> {
        if(s.submitted==1)throw std::runtime_error("partial/unknown submit");
        return std::make_unique<Pending>(s,s.submitted++,false);
    }); }catch(const std::runtime_error&){threw=true;}
    assert(threw && s.live==0 && s.destroyed==1);
    s={}; result=wp::run(rows,o,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);},
        [&]{return s.submitted>=2;});
    assert(!result.success && s.submitted==2 && s.live==0);
    auto bad=rows; bad[0].destination_range.offset=UINT64_MAX;
    threw=false;
    try {wp::run(bad,o,{});}catch(const std::invalid_argument&){threw=true;}
    assert(threw);
    o.instrumentation=wp::Instrumentation::Sampled;o.trace_capacity=1;
    s={};result=wp::run(rows,o,[&](const auto&){return std::make_unique<Pending>(s,s.submitted++,false);});
    assert(result.success && result.trace_overflow && result.trace.size()==1);
    std::cout<<"PASS: native pipeline overlap, out-of-order refill, credits, aliases, failure, cancellation, partial flush\n";
}
