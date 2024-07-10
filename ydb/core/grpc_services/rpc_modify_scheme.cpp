#include "service_scheme.h"

#include "rpc_scheme_base.h"
#include "rpc_common/rpc_common.h"
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

#include <google/protobuf/io/tokenizer.h>

namespace NKikimr {
namespace NGRpcService {

namespace {

bool IsProtobufText(const TString& text) {
    return text.empty() || text.find('{') != TString::npos;
}
struct TErrorCollector : ::google::protobuf::io::ErrorCollector {
    std::vector<std::string> errors;

    void AddError(int line, int column, const TString& message) override {
        errors.push_back(TStringBuilder() << "parse error, at " << line << ":" << column << ": " << message);
    }
    void AddWarning(int line, int column, const TString& message) override {
        errors.push_back(TStringBuilder() << "parse warning, at " << line << ":" << column << ": " << message);
    }
};

bool ParseProtobufText(NKikimrSchemeOp::TModifyScheme* proto, std::vector<std::string>* parseErrors, const TString& text) {
    if (!IsProtobufText(text)) {
        return false;
    }

    bool parseOk = false;
    TErrorCollector errorCollector;
    {
        ::google::protobuf::TextFormat::Parser parser;
        parser.RecordErrorsTo(&errorCollector);
        parseOk = parser.ParseFromString(text, proto);
    }
    if (!parseOk) {
        *parseErrors = std::move(errorCollector.errors);
    }

    return parseOk;
}

}  // anonymous namespace

using namespace NActors;
using namespace Ydb;

using TEvModifySchemeRequest = TGrpcRequestOperationCall<
    Ydb::Scheme::ModifySchemeRequest,
    Ydb::Scheme::ModifySchemeResponse
>;

class TModifySchemeRPC : public TRpcSchemeRequestActor<TModifySchemeRPC, TEvModifySchemeRequest> {
    using TBase = TRpcSchemeRequestActor<TModifySchemeRPC, TEvModifySchemeRequest>;

public:
    TModifySchemeRPC(IRequestOpCtx* msg)
        : TBase(msg) {}

    void Bootstrap(const TActorContext &ctx) {
        TBase::Bootstrap(ctx);

        SendProposeRequest(ctx);
        Become(&TThis::StateWork);
    }

private:
    void SendProposeRequest(const TActorContext &ctx) {
        const auto req = GetProtoRequest();

        if (req->modify_scheme_text().size() == 0) {
            Request_->RaiseIssue(NYql::TIssue("error: empty modify_scheme_text field"));
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        auto proposeRequest = TBase::CreateProposeTransaction();
        NKikimrSchemeOp::TModifyScheme* modifyScheme = proposeRequest->Record.MutableTransaction()->MutableModifyScheme();

        std::vector<std::string> errors;
        if (!ParseProtobufText(modifyScheme, &errors, req->modify_scheme_text())) {
            for (const auto& i : errors) {
                Request_->RaiseIssue(NYql::TIssue{i});
            }
            return Reply(StatusIds::BAD_REQUEST, ctx);
        }

        ctx.Send(MakeTxProxyID(), proposeRequest.release());
    }
};

void DoModifySchemeRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TModifySchemeRPC(p.release()));
}

template<>
IActor* TEvModifySchemeRequest::CreateRpcActor(NKikimr::NGRpcService::IRequestOpCtx* msg) {
    return new TModifySchemeRPC(msg);
}

} // namespace NKikimr
} // namespace NGRpcService
